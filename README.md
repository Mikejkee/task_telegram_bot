### БОТ для офера

Инструкция: 
  - создать бота в https://t.me/BotFather
  - в .env добавить токен бота
  - запустить docker-compose
  - создать в бд postgres таблицу:
    CREATE TABLE tasks (
        id SERIAL PRIMARY KEY,
        task_type TEXT,
    	telegram_id varchar(64)
    )
  - перезапустить докер
