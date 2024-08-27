package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
	"math/rand"
	// "golang.org/x/crypto/bcrypt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func getDBConnection() (*sql.DB, error) {
	// 连接到对应数据库"用户名:密码@协议(127.0.0.1:端口号)/数据库名"
	dsn := "root:root@tcp(127.0.0.1:3306)/db_bank"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

var accountLocks = sync.Map{} // 使用 sync.Map 存储每个账户的锁

// 获取账户的锁对象
func getAccountLock(accountID int) *sync.Mutex {
	// 如果账户锁不存在，则创建一个新的锁
	lock, _ := accountLocks.LoadOrStore(accountID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// 锁定两个账户，按照 ID 排序后加锁，避免死锁
func lockAccounts(fromAccountID, toAccountID int) (*sync.Mutex, *sync.Mutex) {
	if fromAccountID < toAccountID {
		return getAccountLock(fromAccountID), getAccountLock(toAccountID)
	} else {
		return getAccountLock(toAccountID), getAccountLock(fromAccountID)
	}
}

// Transaction 交易记录结构体
type Transaction struct {
	ID              int 
    Amount          float64
	FromAccountID   int
	FromAccountName string
	FromAccountNo   string
	ToAccountID     int
	ToAccountName   string
	ToAccountNo		string
	TimeStartstamp 	string
	TimeEndstamp 	string
	Status          string
}


// 用户信息结构体
type User struct {
	Username       string
	AccountNo      string
	FrozeBalance   float64
	Balance        float64
}

// 定义接口
type TransferService interface {
	Transfer(fromAccountID int, toAccountID int, amount float64) error
}

type TransactionService interface {
	SelectTransactions(fromAccountID int) ([]Transaction, error)
}

type BankService struct {
    db *sql.DB
}

func NewBankService(db *sql.DB) *BankService {
    return &BankService{db: db}
}

// 查询用户相关信息函数
func SelectAccount(db *sql.DB, UserID int) (User,error){
	
	var u User
	query := `SELECT username, accountNo, frozeBalance, balance FROM t_account WHERE id = ?`
	err := db.QueryRow(query, UserID).Scan(&u.Username, &u.AccountNo, &u.FrozeBalance, &u.Balance)
	if err != nil {
		if err == sql.ErrNoRows{
			return User{}, err
		}
	}
	return u, nil
}

// 交易流水表相关函数
// func Createtransactions(db *sql.DB,) {

// }

// 转账功能
func Transfer(db *sql.DB, FromAccountID int, ToAccountID int, Amount float64) error{
	// 锁定账户，按照 ID 大小排序后加锁
	lock1, lock2 := lockAccounts(FromAccountID, ToAccountID)
	lock1.Lock()
	defer lock1.Unlock()
	lock2.Lock()
	defer lock2.Unlock()
	
	// 读取 转出账户余额和冻结金额 并做出相应的判断处理
	s1, err := SelectAccount(db, FromAccountID)
	if err != nil {
		return fmt.Errorf("获取转出账户信息失败: %v", err)
	}
	s2, err := SelectAccount(db, ToAccountID)
	if err != nil {
		return fmt.Errorf("获取转入账户信息失败: %v", err)
	}

	// 判断账户余额是否充足
	if Amount > s1.Balance {
		return fmt.Errorf("账户余额不足")
	}
	
	// 开启事务
	tx, err :=db.Begin()
	if err != nil {
		fmt.Println("事务启动失败:", err)
		return err
	}


	// 扣减转出账户的余额
	updateFromAccountBQuery := `UPDATE t_account SET balance = balance - ? WHERE id = ?`
    _, err = tx.Exec(updateFromAccountBQuery, Amount, FromAccountID)
    if err != nil {
		log.Fatal(err)
        tx.Rollback()
        return fmt.Errorf("扣减转出账户余额失败: %v", err)
    }

	// 增加转出账户冻结的金额
	updateFromAccountFBQuery := `UPDATE t_account SET frozeBalance = frozeBalance + ? WHERE id = ?` 
	_, err = tx.Exec(updateFromAccountFBQuery, Amount, FromAccountID)
    if err != nil {
		log.Fatal(err)
        tx.Rollback()
        return fmt.Errorf("增加转出账户冻结的金额失败: %v", err)
    }
	
	
	// 创建流水记录
	updateTransactionsQuery := `INSERT INTO t_transactions (
		amount, from_account_id, from_account_name, from_accountNo, to_account_id, to_account_name, to_accountNo, time_start_stamp, status
	) VALUES(?, ?, ?, ?, ?, ?, ?, NOW(), 'pending')`
	res, err := tx.Exec(updateTransactionsQuery, Amount, FromAccountID, s1.Username, s1.AccountNo, ToAccountID, s2.Username, s2.AccountNo)
	if err != nil {
		log.Fatal(err)
		tx.Rollback()
		return fmt.Errorf("创建流水失败: %v", err)
	}

	lastID, err := res.LastInsertId()
	if err != nil {
		log.Fatal(err)
		tx.Rollback()
		return fmt.Errorf("获取当前交易流水id失败:%v", err)
	}
	
	// 交易成功
	updateFromAccountResCQuery := `UPDATE t_transactions SET time_end_stamp = NOW(), status = 'completed' WHERE id = ?`

	// 交易失败
	// updateFromAccountResFQuery := `UPDATE t_transactions SET time_end_stamp = NOW(), status = 'failed' WHERE id = ?`



	// 增加转入账户余额
	updateToAccountBQuery := `UPDATE t_account SET balance = balance + ? WHERE id = ?`
	_, err = tx.Exec(updateToAccountBQuery, Amount, ToAccountID)
	if err != nil {
		log.Fatal(err)
		tx.Rollback()
        return fmt.Errorf("交易失败: %v", err)
    }

	// 扣减转出账户冻结金额
	updateFromAccountFB2Query :=`UPDATE t_account SET frozeBalance = frozeBalance - ? WHERE id = ?`
	_, err = tx.Exec(updateFromAccountFB2Query, Amount, FromAccountID)
	if err != nil {
		log.Fatal(err)
		tx.Rollback()
		return fmt.Errorf("修改冻结金额失败: %v", err)
	}
	
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("提交事务失败: %v", err)
	}

	time.Sleep(time.Duration(rand.Intn(4) + 1) * time.Second)

	_, err = db.Exec(updateFromAccountResCQuery, lastID)
	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("更新交易状态失败: %v", err)
	}

	fmt.Println("交易成功")
	return nil
}


// 查询交易流水表功能
func SelectTransactions(db *sql.DB, UserId int) ([]Transaction, error) {
    // 构建查询语句，筛选出所有 from_account_id 等于 UserId 的记录
    query := `SELECT id, amount, from_account_id, from_account_name, from_accountNo, to_account_id, to_account_name, to_accountNo, time_start_stamp, time_end_stamp, status FROM t_transactions WHERE from_account_id = ? ORDER BY time_start_stamp DESC`

    var transactions []Transaction

    // 执行查询
    rows, err := db.Query(query, UserId)
    if err != nil {
        return nil, fmt.Errorf("查询流水记录失败: %v", err)
    }
    defer rows.Close()

    // 遍历查询结果
    for rows.Next() {
        var transaction Transaction
        // 扫描每一行数据到 Transaction 结构体
        if err := rows.Scan(&transaction.ID, &transaction.Amount, &transaction.FromAccountID, &transaction.FromAccountName, &transaction.FromAccountNo, &transaction.ToAccountID, &transaction.ToAccountName, &transaction.ToAccountNo, &transaction.TimeStartstamp, &transaction.TimeEndstamp, &transaction.Status); err != nil {
            return nil, fmt.Errorf("扫描结果失败: %v", err)
        }
        transactions = append(transactions, transaction)
    }

    // 检查是否有可能在迭代时发生错误
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("迭代查询结果失败: %v", err)
    }

    return transactions, nil
}


// 计算处理时长并筛选处理时长超过3秒的交易记录
func SelectTransactionsExceeding3s(db *sql.DB, userId int) ([]Transaction, error) {
	// 查询所有记录
	query := `SELECT id, amount, from_account_id, from_account_name, from_accountNo, to_account_id, to_account_name, to_accountNo, time_start_stamp, time_end_stamp, status 
              FROM t_transactions 
              WHERE from_account_id = ? ORDER BY time_start_stamp DESC`

	rows, err := db.Query(query, userId)
	if err != nil {
		return nil, fmt.Errorf("查询流水记录失败: %v", err)
	}
	defer rows.Close()

	var transactions []Transaction

	for rows.Next() {
		var transaction Transaction
		var startTimeStr, endTimeStr string

		// 扫描数据，time_start_stamp 和 time_end_stamp 作为字符串
		if err := rows.Scan(&transaction.ID, &transaction.Amount, &transaction.FromAccountID, &transaction.FromAccountName, &transaction.FromAccountNo,
			&transaction.ToAccountID, &transaction.ToAccountName, &transaction.ToAccountNo, &startTimeStr, &endTimeStr, &transaction.Status); err != nil {
			return nil, fmt.Errorf("扫描结果失败: %v", err)
		}

		// 将字符串转换为 time.Time 类型
		startTime, err := time.Parse("2006-01-02 15:04:05", startTimeStr)
		if err != nil {
			return nil, fmt.Errorf("解析开始时间失败: %v", err)
		}

		endTime, err := time.Parse("2006-01-02 15:04:05", endTimeStr)
		if err != nil {
			return nil, fmt.Errorf("解析结束时间失败: %v", err)
		}

		// 计算处理时长
		duration := endTime.Sub(startTime).Seconds()

		// 处理时长超过3秒，添加到结果集
		if duration > 3 {
			transaction.TimeStartstamp = startTime.Format(time.RFC3339)
			transaction.TimeEndstamp = endTime.Format(time.RFC3339)
			transactions = append(transactions, transaction)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("迭代查询结果失败: %v", err)
	}

	return transactions, nil
}



func main() {
	// 连接数据库
	db, err := getDBConnection()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// 测试连接是否成功
	err = db.Ping()
	if err != nil {
		log.Fatal("数据库连接失败:", err)
		return
	}
    fmt.Println("连接成功！")

	// res, err := SelectTransactions(db, 2)
	// if err != nil {
	// 	log.Fatal(err)
	// 	return
	// }

	// for _, record := range res {
	// 	fmt.Printf("Transaction ID: %d, FromAccountID: %d, FromAccountName: %s, FromAccountNo: %s, ToAccountID: %d, ToAccountName: %s, ToAccountNo: %s, TimeStartStamp: %s, TimeEndStamp: %s, Status: %s\n,",record.ID, record.FromAccountID, record.FromAccountName, record.FromAccountNo, record.ToAccountID, record.ToAccountName, record.ToAccountNo, record.TimeStartstamp, record.TimeEndstamp, record.Status)
	// }

	// 查询处理时长超过3秒的交易记录
	transactions, err := SelectTransactionsExceeding3s(db, 2)
	if err != nil {
		log.Fatal(err)
		return
	}

	for _, record := range transactions {
		fmt.Printf("Transaction ID: %d, FromAccountID: %d, FromAccountName: %s, FromAccountNo: %s, ToAccountID: %d, ToAccountName: %s, ToAccountNo: %s, TimeStartStamp: %s, TimeEndStamp: %s, Status: %s\n",
			record.ID, record.FromAccountID, record.FromAccountName, record.FromAccountNo, record.ToAccountID, record.ToAccountName, record.ToAccountNo, record.TimeStartstamp, record.TimeEndstamp, record.Status)
	}
	
	
	
}