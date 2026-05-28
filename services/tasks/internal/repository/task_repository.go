package repository

import (
	"context"
	"database/sql"
	"fmt"

	"tech-ip-sem2/shared/models"
)

type TaskRepository struct {
	db *sql.DB
}

func NewTaskRepository(db *sql.DB) (*TaskRepository, error) {
	if err := createTable(db); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return &TaskRepository{db: db}, nil
}

func createTable(db *sql.DB) error {
	query := `
        CREATE TABLE IF NOT EXISTS tasks (
            id VARCHAR(36) PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            due_date VARCHAR(50),
            done BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `
	_, err := db.Exec(query)
	return err
}

func (r *TaskRepository) Create(ctx context.Context, task models.Task) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO tasks (id, title, description, due_date, done, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`,
		task.ID,
		task.Title,
		task.Description,
		task.DueDate,
		task.Done,
		task.CreatedAt,
		task.UpdatedAt,
	)
	return err
}

func (r *TaskRepository) GetAll(ctx context.Context) ([]models.Task, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, title, description, due_date, done, created_at, updated_at FROM tasks
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []models.Task

	for rows.Next() {
		var t models.Task

		err := rows.Scan(
			&t.ID,
			&t.Title,
			&t.Description,
			&t.DueDate,
			&t.Done,
			&t.CreatedAt,
			&t.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (r *TaskRepository) GetByID(ctx context.Context, id string) (models.Task, error) {
	var t models.Task

	err := r.db.QueryRowContext(ctx, `
		SELECT id, title, description, due_date, done, created_at, updated_at
		FROM tasks WHERE id=$1
	`, id).Scan(
		&t.ID,
		&t.Title,
		&t.Description,
		&t.DueDate,
		&t.Done,
		&t.CreatedAt,
		&t.UpdatedAt,
	)

	return t, err
}

func (r *TaskRepository) Update(ctx context.Context, task models.Task) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE tasks
		SET title=$1, description=$2, due_date=$3, done=$4, updated_at=$5
		WHERE id=$6
	`,
		task.Title,
		task.Description,
		task.DueDate,
		task.Done,
		task.UpdatedAt,
		task.ID,
	)

	return err
}

func (r *TaskRepository) Delete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM tasks WHERE id=$1
	`, id)

	return err
}

// SearchByTitle ищет задачи по заголовку
func (r *TaskRepository) SearchByTitle(ctx context.Context, title string) ([]models.Task, error) {
	query := `
        SELECT id, title, description, due_date, done, created_at, updated_at 
        FROM tasks 
        WHERE title ILIKE $1
        ORDER BY created_at DESC
    `

	searchPattern := "%" + title + "%"

	rows, err := r.db.QueryContext(ctx, query, searchPattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []models.Task
	for rows.Next() {
		var t models.Task
		err := rows.Scan(
			&t.ID,
			&t.Title,
			&t.Description,
			&t.DueDate,
			&t.Done,
			&t.CreatedAt,
			&t.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}

	return tasks, nil
}
