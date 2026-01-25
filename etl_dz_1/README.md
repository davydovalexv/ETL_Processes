# Контейнеризированный ETL-проект с Apache Airflow и PostgreSQL

Проект реализует ETL-пайплайн для извлечения данных из JSON-источника, их преобразования и загрузки в таблицу PostgreSQL с использованием Apache Airflow.

## Архитектура проекта

```
├── dags/                    # DAG-файлы Apache Airflow
│   └── json_etl_pipeline.py # Основной ETL-пайплайн
├── docker-compose.yml       # Конфигурация Docker Compose
├── Dockerfile              # Кастомный образ Airflow с зависимостями
├── init.sql               # SQL-скрипт для инициализации БД
├── requirements.txt       # Python-зависимости
└── README.md             # Документация
```

## Функциональные возможности

1. **Extract**: Чтение данных из JSON API (https://dummyjson.com/products)
2. **Transform**: Преобразование вложенных структур в табличный формат
3. **Load**: Загрузка данных в PostgreSQL с предотвращением дублирования
4. **Validation**: Проверка качества загруженных данных
5. **Logging**: Отслеживание выполнения ETL-процессов

## Требования к системе

- Docker 20.10+
- Docker Compose 2.0+
- 4GB свободной оперативной памяти
- 2GB свободного места на диске

## Быстрый старт

### 1. Клонирование и настройка

```bash
# Клонирование проекта (если необходимо)
git clone <repository-url>
cd <project-directory>
```

### 2. Запуск контейнеров

```bash
# Запуск всех сервисов
docker-compose up -d
```

Сервисы будут запущены в следующем порядке:
1. PostgreSQL (порт 5432)
2. Apache Airflow Web Server (порт 8080)
3. Apache Airflow Scheduler

### 3. Проверка работоспособности

После запуска проверьте статус сервисов:

```bash
# Проверка статуса контейнеров
docker-compose ps

# Просмотр логов
docker-compose logs -f
```

### 4. Доступ к интерфейсам

- **Apache Airflow UI**: http://localhost:8080
  - Логин: `admin`
  - Пароль: `admin`

- **PostgreSQL**:
  - Хост: `localhost`
  - Порт: `5432`
  - База данных: `airflow` (для метаданных Airflow)
  - База данных: `etl_data` (для ETL-данных)
  - Пользователь: `airflow`
  - Пароль: `airflow`

## Использование ETL-пайплайна

### DAG: json_etl_pipeline

Пайплайн автоматически запускается ежедневно и состоит из следующих этапов:

1. **start** → **extract** → **transform** → **load** → **validate** → **end**

### Ручной запуск DAG

1. Откройте Airflow UI (http://localhost:8080)
2. Найдите DAG "json_etl_pipeline"
3. Нажмите кнопку "Trigger DAG"
4. Следите за выполнением в графе задач

### Проверка результатов

После успешного выполнения DAG данные будут загружены в таблицу `products` базы данных `etl_data`.

```sql
-- Подключение к PostgreSQL
docker exec -it $(docker-compose ps -q postgres) psql -U airflow -d etl_data

-- Проверка загруженных данных
SELECT COUNT(*) as total_products FROM products;
SELECT * FROM products LIMIT 5;

-- Просмотр истории ETL-запусков
SELECT * FROM etl_runs ORDER BY started_at DESC;
```

## Структура данных

### Таблица products

| Поле | Тип | Описание |
|------|-----|----------|
| id | SERIAL | Автоинкрементный первичный ключ |
| product_id | VARCHAR(50) | Уникальный идентификатор продукта |
| name | VARCHAR(255) | Название продукта |
| category | VARCHAR(100) | Категория продукта |
| price | DECIMAL(10,2) | Цена продукта |
| rating | DECIMAL(3,2) | Рейтинг продукта |
| stock | INTEGER | Количество на складе |
| brand | VARCHAR(100) | Бренд продукта |
| description | TEXT | Описание продукта |
| created_at | TIMESTAMP | Время создания записи |
| updated_at | TIMESTAMP | Время последнего обновления |

### Таблица etl_runs

| Поле | Тип | Описание |
|------|-----|----------|
| run_id | SERIAL | Идентификатор запуска |
| dag_id | VARCHAR(100) | Идентификатор DAG |
| execution_date | TIMESTAMP | Дата выполнения |
| status | VARCHAR(20) | Статус выполнения |
| records_processed | INTEGER | Количество обработанных записей |
| started_at | TIMESTAMP | Время начала |
| completed_at | TIMESTAMP | Время завершения |
| error_message | TEXT | Сообщение об ошибке (если есть) |

## Особенности реализации

### Предотвращение дублирования

Пайплайн использует механизм upsert (UPDATE/INSERT) для предотвращения дублирования данных:
- Проверяется существование записи по `product_id`
- Если запись существует - обновляются поля
- Если записи нет - создается новая

### Обработка ошибок

- Автоматические повторные попытки при сбоях
- Логирование ошибок в таблицу `etl_runs`
- Валидация данных после загрузки

### Масштабируемость

- Загрузка данных пакетами по 100 записей
- Индексы для ускорения запросов
- Триггеры для автоматического обновления временных меток

## Администрирование

### Остановка сервисов

```bash
# Остановка всех контейнеров
docker-compose down

# Остановка с удалением томов
docker-compose down -v
```

### Обновление зависимостей

1. Отредактируйте `requirements.txt`
2. Перестройте образы:
   ```bash
   docker-compose build --no-cache
   docker-compose up -d
   ```

### Просмотр логов

```bash
# Логи Airflow Web Server
docker-compose logs airflow-webserver

# Логи Airflow Scheduler
docker-compose logs airflow-scheduler

# Логи PostgreSQL
docker-compose logs postgres
```

### Резервное копирование данных

```bash
# Дамп базы данных etl_data
docker exec $(docker-compose ps -q postgres) pg_dump -U airflow etl_data > backup_$(date +%Y%m%d).sql
```

## Устранение неполадок

### Проблема: Контейнеры не запускаются

**Решение:**
```bash
# Проверка доступности портов
netstat -tulpn | grep -E '(5432|8080)'

# Очистка предыдущих контейнеров
docker-compose down -v
docker system prune -f
```

### Проблема: DAG не появляется в Airflow UI

**Решение:**
```bash
# Перезапуск Airflow
docker-compose restart airflow-webserver airflow-scheduler

# Проверка файлов DAG
docker exec $(docker-compose ps -q airflow-webserver) ls -la /opt/airflow/dags/
```

### Проблема: Ошибка подключения к PostgreSQL

**Решение:**
```bash
# Проверка состояния PostgreSQL
docker-compose exec postgres pg_isready -U airflow

# Перезапуск PostgreSQL
docker-compose restart postgres
```

## Расширение функциональности

### Добавление нового источника данных

1. Создайте новый DAG в директории `dags/`
2. Реализуйте функции extract, transform, load
3. Добавьте необходимые зависимости в `requirements.txt`

### Изменение схемы данных

1. Отредактируйте `init.sql`
2. Пересоздайте контейнеры:
   ```bash
   docker-compose down -v
   docker-compose up -d
   ```

### Настройка расписания

Измените параметр `schedule_interval` в файле `dags/json_etl_pipeline.py`:
- `@daily` - ежедневно
- `@hourly` - ежечасно
- `0 0 * * *` - по cron-расписанию

## Лицензия

Проект распространяется под MIT License.

## Контакты

Для вопросов и предложений обращайтесь к разработчику проекта.