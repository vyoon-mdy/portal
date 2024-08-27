import (
	"context"
	// 其他导入省略
)

// Transfer 超时传参的更新
func Transfer(db *sql.DB, FromAccountID int, ToAccountID int, Amount float64, timeout time.Duration) error {
	// 创建带超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 锁定账户，按照 ID 大小排序后加锁
	lock1, lock2 := lockAccounts(FromAccountID, ToAccountID)
	lock1.Lock()
	defer lock1.Unlock()
	lock2.Lock()
	defer lock2.Unlock()
	
	// 开始事务
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("事务启动失败: %v", err)
	}

	// 获取账户信息的更新
	s1, err := SelectAccount(db, FromAccountID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("获取转出账户信息失败: %v", err)
	}
	s2, err := SelectAccount(db, ToAccountID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("获取转入账户信息失败: %v", err)
	}

	// 账户余额检查
	if Amount > s1.Balance {
		tx.Rollback()
		return fmt.Errorf("账户余额不足")
	}

	// 开始执行转账操作
	// 扣减余额的操作
	updateFromAccountBQuery := `UPDATE t_account SET balance = balance - ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateFromAccountBQuery, Amount, FromAccountID)
	if err != nil {
		updateTransactionStatus(tx, lastID, "failed")
		tx.Rollback()
		return fmt.Errorf("扣减转出账户余额失败: %v", err)
	}

	// 增加冻结金额
	updateFromAccountFBQuery := `UPDATE t_account SET frozeBalance = frozeBalance + ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateFromAccountFBQuery, Amount, FromAccountID)
	if err != nil {
		updateTransactionStatus(tx, lastID, "failed")
		tx.Rollback()
		return fmt.Errorf("增加转出账户冻结的金额失败: %v", err)
	}

	// 创建流水记录
	updateTransactionsQuery := `INSERT INTO t_transactions (
		amount, from_account_id, from_account_name, from_accountNo, to_account_id, to_account_name, to_accountNo, time_start_stamp, status
	) VALUES(?, ?, ?, ?, ?, ?, ?, NOW(), 'pending')`
	res, err := tx.ExecContext(ctx, updateTransactionsQuery, Amount, FromAccountID, s1.Username, s1.AccountNo, ToAccountID, s2.Username, s2.AccountNo)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("创建流水失败: %v", err)
	}

	lastID, err := res.LastInsertId()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("获取当前交易流水id失败: %v", err)
	}

	// 增加转入账户余额
	updateToAccountBQuery := `UPDATE t_account SET balance = balance + ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateToAccountBQuery, Amount, ToAccountID)
	if err != nil {
		updateTransactionStatus(tx, lastID, "failed")
		tx.Rollback()
		return fmt.Errorf("交易失败: %v", err)
	}

	// 扣减转出账户冻结金额
	updateFromAccountFB2Query := `UPDATE t_account SET frozeBalance = frozeBalance - ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateFromAccountFB2Query, Amount, FromAccountID)
	if err != nil {
		updateTransactionStatus(tx, lastID, "failed")
		tx.Rollback()
		return fmt.Errorf("修改冻结金额失败: %v", err)
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			updateTransactionStatus(tx, lastID, "failed")
			return fmt.Errorf("转账超时，操作已回滚: %v", err)
		}
		return fmt.Errorf("提交事务失败: %v", err)
	}

	// 交易成功，更新交易记录状态为 completed
	updateTransactionStatus(db, lastID, "completed")

	fmt.Println("交易成功")
	return nil
}