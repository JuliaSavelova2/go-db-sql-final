package main

import (
	"database/sql"
	"log"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	const query = `
		INSERT INTO 
			parcel (client, status, address, created_at)
		VALUES
			(:client, :status, :address, :created_at)
	`

	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	response, err := s.db.Exec(query,
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		return 0, nil
	}

	id, err := response.LastInsertId()
	if err != nil {
		return 0, nil
	}

	// верните идентификатор последней добавленной записи
	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	const query = `
		SELECT * FROM parcel 
		WHERE number = :number
	`

	// заполните объект Parcel данными из таблицы
	p := Parcel{}

	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow(query, sql.Named("number", number))
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return p, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	const query = `
		SELECT * FROM parcel
		WHERE client = :client
	`

	// заполните срез Parcel данными из таблицы
	var res []Parcel

	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	rows, err := s.db.Query(query, sql.Named("client", client))
	if err != nil {
		return res, nil
	}
	defer rows.Close()

	for rows.Next() {
		p := Parcel{}

		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		res = append(res, p)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	const query = `
		UPDATE parcel
		SET
			status = :status
		WHERE 
			number = :number
	`

	// реализуйте обновление статуса в таблице parcel
	_, err := s.db.Exec(query, sql.Named("status", status), sql.Named("number", number))
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	const query = `
		UPDATE parcel
		SET
			address = :address
		WHERE 
			status = 'registered' AND
			number = :number
	`

	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	_, err := s.db.Exec(query, sql.Named("address", address), sql.Named("number", number))
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	const query = `
		DELETE FROM parcel
		WHERE 
			status = 'registered' AND
			number = :number
	`

	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	_, err := s.db.Exec(query, sql.Named("number", number))
	if err != nil {
		return err
	}

	return nil
}
