# Глава 1: Подключение к базе данных

Представьте, что вы строите новый дом, и вам нужно проложить воду, электричество и газ. У каждого из этих сервисов есть свои специальные трубы и провода, но внутри дома вы используете стандартные розетки и краны. Вы прямо не заботитесь о сложной сети за пределами; вы просто подключаетесь к основным линиям.

В мире программного обеспечения, когда вы хотите работать с базой данных, это похоже на то. Существует множество различных типов баз данных (например, PostgreSQL, MySQL, Snowflake, BigQuery), и у каждой свои "трубы и провода" для связи. Все они понимают SQL (Structured Query Language), но способ *общения* с ними на низком уровне может значительно отличаться.

Именно здесь вступает в дело `sqeleton` и его **Подключение к базе данных**. Он выступает в роли универсального адаптера или "главного входа". Вместо того чтобы учиться конкретным деталям подключения и правилам взаимодействия для каждой базы данных, вы используете `sqeleton`, чтобы он управлял всей этой сложностью за вас. Вы просто говорите `sqeleton`, *какую* базу данных хотите использовать и *где* она находится, и `sqeleton` настраивает путь, позволяя без усилий отправлять команды и получать результаты.

Главная проблема, которую решает `sqeleton`, — это **абстракция**: скрытие сложных, низкоуровневых деталей взаимодействия с разными базами данных, чтобы вы могли сосредоточиться на написании своих SQL-запросов.

## Подключение к вашей базе данных

Давайте начнем с простой, распространенной задачи: подключиться к базе данных и выполнить простое вычисление, например, `SELECT 1 + 1`. Это отличный способ убедиться, что ваше подключение работает!

Основная функция для этого — `sqeleton.connect()`.

### Как указать `sqeleton`, где находится ваша база данных

`sqeleton.connect()` очень гибкая. Вы можете указать адрес вашей базы данных двумя распространенными способами:

1.  **В виде строки соединения (URI):** Это одна текстовая строка, содержащая всю необходимую информацию, как веб-адрес. Обычно она выглядит примерно так: `driver://user:password@host:port/database_name`.

    Например, чтобы подключиться к базе данных PostgreSQL, работающей на вашем локальном компьютере:

    ```python
    from sqeleton import connect

    # Подключение к базе данных PostgreSQL
    db_connection = connect("postgresql://user:password@localhost:5432 mydatabase")

    print(f"Подключено к: {db_connection.name}")
    ```

    После выполнения этого кода `db_connection` будет содержать активную связь с вашей базой данных PostgreSQL.

2.  **В виде словаря Python:** Если предпочитаете, вы можете предоставить параметры подключения в виде словаря, где каждый элемент — ключ-значение, например `host`, `user`, `password`, `database`. Это удобно для более сложных конфигураций или при загрузке настроек из файла.

    Например, подключение к базе данных MySQL:

    ```python
    from sqeleton import connect

    # Подключение к базе данных MySQL с помощью словаря
    db_config = {
        "driver": "mysql",
        "host": "localhost",
        "user": "myuser",
        "password": "mypassword",
        "database": "testdb"
    }
    db_connection = connect(db_config)

    print(f"Подключено к: {db_connection.name}")
    ```

    `sqeleton` поддерживает множество популярных баз данных. Ниже приведены примеры значений `driver` для различных типов баз данных:

    | Тип базы данных | Название драйвера | Пример строки соединения (URI) |
    | :-------------- | :---------------- | :------------------------------ |
    | PostgreSQL      | `postgresql`      | `postgresql://user:pass@host:port/database` |
    | MySQL           | `mysql`           | `mysql://user:pass@host:port/database` |
    | Snowflake       | `snowflake`       | `snowflake://user:pass@account/db/schema?warehouse=WAREHOUSE` |
    | BigQuery        | `bigquery`        | `bigquery://project/dataset` |
    | Databricks      | `databricks`      | `databricks://:<access_token>@<server_hostname>/<http_path>` |
    | DuckDB          | `duckdb`          | `duckdb://<dbname>@<filepath>` |
    | Redshift        | `redshift`        | `redshift://user:pass@host/database` |
    | Presto          | `presto`          | `presto://user@host/catalog/schema` |
    | Trino           | `trino`           | `trino://user@host/catalog/schema` |
    | Clickhouse      | `clickhouse`      | `clickhouse://user:pass@host/database` |
    | Oracle          | `oracle`          | `oracle://user:pass@host/database` |
    | Vertica         | `vertica`         | `vertica://user:pass@host/database` |

### Отправка вашего первого запроса

Как только у вас есть объект `db_connection`, вы можете использовать его метод `.query()`, чтобы отправлять SQL-команды.

Давайте используем наш предыдущий пример: `SELECT 1 + 1`.

```python
from sqeleton import connect

# Подключение к выбранной базе данных (например, PostgreSQL)
db = connect("postgresql://user:password@localhost:5432/mydatabase")

# Отправка простого SQL-запроса
result = db.query("SELECT 1 + 1")

# Вывод результата
# Для таких простых запросов, как этот, результат часто содержит только значение
print(f"Результат 'SELECT 1 + 1': {result}")

# Не забудьте закрыть соединение, когда закончите!
db.close()
```

Вывод:
```
Результат 'SELECT 1 + 1': 2
```

Этот `result` может быть одним значением, списком строк или более сложным объектом, в зависимости от запроса. Пока что достаточно знать, что `db.query()` — это ваш способ взаимодействовать с базой данных.

## Как работает `sqeleton.connect()` — взгляд изнутри

Чтобы по-настоящему понять `sqeleton`, полезно заглянуть за кулисы. Когда вы вызываете `sqeleton.connect()`, происходит ряд шагов для установления бесшовного соединения.

### Общий поток процесса

Давайте визуализируем процесс:

```mermaid
sequenceDiagram
    participant YourCode as Ваш Python код
    participant GlobalConnect as sqeleton.connect()
    participant ConnectObject as Объект подключения (sqeleton/databases/_connect.py)
    participant DatabaseClass as Класс базы данных (например, PostgreSQL)
    participant DatabaseDriver as Драйвер базы данных (например, psycopg2)
    participant ActualDB as Реальная база данных

    YourCode->>GlobalConnect: Вызов connect("postgresql://...")
    GlobalConnect->>ConnectObject: Передача данных в `__call__`
    Note over ConnectObject: 1. Разбор URI/словаря<br/>2. Определение типа базы данных (например, "postgresql")<br/>3. Поиск соответствующего класса базы данных
    ConnectObject->>DatabaseClass: Создание экземпляра нужного класса базы (например, PostgreSQL) с параметрами
    DatabaseClass->>DatabaseDriver: Установка низкоуровневого соединения через внешний драйвер (например, psycopg2)
    DatabaseDriver->>ActualDB: Отправка запроса на подключение
    ActualDB-->>DatabaseDriver: Успешное соединение
    DatabaseDriver-->>DatabaseClass: Возврат объекта соединения
    DatabaseClass-->>ConnectObject: Возврат объекта `Database`
    ConnectObject-->>GlobalConnect: Возврат объекта `Database`
    GlobalConnect-->>YourCode: Получение объекта `db_connection`
```

### Глубже в исходный код

Рассмотрим, как происходит подключение в исходных файлах `sqeleton`:

1.  **Функция `connect`:**
    Функция `connect`, которую вы импортируете из `sqeleton`, на самом деле — это экземпляр класса `Connect`. Это позволяет `sqeleton` управлять разными типами баз данных.

    ```python
    # from sqeleton/__init__.py
    from .databases import connect

    # из sqeleton/databases/__init__.py
    from ._connect import Connect
    connect = Connect()  # Это объект, который вызываете!
    ```

2.  **Класс `Connect` (`sqeleton/databases/_connect.py`):**
    Этот класс имеет особый метод `__call__`, который позволяет его экземпляру (`connect`) вызываться как функция. Он отвечает за разбор входных данных (URI или словарь) и выбор нужного класса базы данных.

    ```python
    # из sqeleton/databases/_connect.py

    # ... (импорты и другой код) ...

    DATABASE_BY_SCHEME = {
        "postgresql": PostgreSQL,  # связывает схему "postgresql" с классом PostgreSQL
        "mysql": MySQL,            # связывает схему "mysql" с классом MySQL
        # ... множество других отображений баз данных
    }

    class Connect:
        def __init__(self, database_by_scheme: Dict[str, Database] = DATABASE_BY_SCHEME):
            self.database_by_scheme = database_by_scheme
            # ... (другие инициализации) ...

        def __call__(self, db_conf: Union[str, dict], thread_count: Optional[int] = 1, shared: bool = True) -> Database:
            if isinstance(db_conf, str):
                conn = self.connect_to_uri(db_conf, thread_count)
            elif isinstance(db_conf, dict):
                conn = self.connect_with_dict(db_conf, thread_count)
            # ... (обработка ошибок и кэширование) ...
            return conn

        def connect_to_uri(self, db_uri: str, thread_count: Optional[int] = 1) -> Database:
            dsn = dsnparse.parse(db_uri)  # Разбор URI
            (scheme,) = dsn.schemes       # Извлечение схемы (например, "postgresql")
            cls = self.database_by_scheme[scheme]  # Получение нужного класса базы данных
            
            # ... (выделение host, user, password, и т.д. из dsn) ...

            if issubclass(cls, ThreadedDatabase):  # Проверка, является ли база потокобезопасной
                db = cls(thread_count=thread_count, **kw)
            else:
                db = cls(**kw)
            return self._connection_created(db)

        # Метод connect_with_dict обрабатывает словарь аналогично
        # ...
    ```
    Этот класс `Connect` — это контроллер трафика. Он использует `dsnparse`, чтобы понять ваш строку соединения, и затем использует словарь (`DATABASE_BY_SCHEME`), чтобы найти правильный класс базы данных (например, `PostgreSQL` или `MySQL`) для создания экземпляра.

3.  **Классы `Database` (`sqeleton/databases/base.py` и специальные файлы):**
    Все конкретные драйверы баз данных (например, `PostgreSQL`, `MySQL`, `Snowflake`) наследуют абстрактный класс `Database` (или `ThreadedDatabase`). Этот базовый класс задает общий план, как должны вести себя все базы данных в `sqeleton`, включая метод `_query`.

    ```python
    # из sqeleton/abcs/database_types.py (определение абстрактного базового класса)
    class AbstractDatabase(Generic[T_Dialect]):
        @property
        @abstractmethod
        def name(self) -> str:
            # ...
        @property
        @abstractmethod
        def dialect(self) -> T_Dialect:
            # ...
        @abstractmethod
        def _query(self, sql_code: Any) -> list:
            "Отправить запрос в базу и вернуть результат"
        # ... другие абстрактные методы

    # из sqeleton/databases/base.py (базовая реализация)
    class Database(AbstractDatabase[T]):
        # ... (общие методы, такие как query, select_table_schema) ...

        def _query_conn(self, conn, sql_code: SqlCode) -> Optional[QueryResult]:
            # Этот метод принимает низкоуровневое соединение и выполняет SQL
            c = conn.cursor()
            callback = partial(self._query_cursor, c)
            return apply_query(callback, sql_code)

        def query(self, query_input, res_type=None):
            # Это публичный метод, который вызываете (db.query(...))
            # Он компилирует запрос (с помощью Compiler) и вызывает _query
            # ...
            sql_code = Compiler(self).compile_with_args(query_input)
            res = self._query(sql_code)  # вызывает конкретный _query конкретной базы
            # ... обработка результата ...
            return res

    # из sqeleton/databases/postgresql.py (конкретная реализация базы)
    import psycopg2  # низкоуровенный драйвер PostgreSQL для Python

    class PostgreSQL(ThreadedDatabase):
        dialect = PostgresqlDialect()  # Каждая база имеет свой диалект
        # ...

        def create_connection(self):  # Специфический метод для ThreadedDatabase
            pg = import_postgresql()  # Импорт psycopg2
            try:
                c = pg.connect(**self._args)  # Установка соединения
                return c
            except pg.OperationalError as e:
                raise ConnectError(*e.args) from e

        def _query(self, sql_code: Union[str, ThreadLocalInterpreter]):
            # PostgreSQL наследует _query_conn из ThreadedDatabase, которая
            # в конечном итоге вызывает create_connection для получения низкоуровневого соединения
            return self._query_conn(self._conn, sql_code)
    ```

    Каждый конкретный класс базы (например, `PostgreSQL`) знает, как использовать свой драйвер (например, `psycopg2`) для установления реального соединения. Когда вы вызываете `db.query()`, `sqeleton` сначала использует [SQL-компилятор](05_sql_compiler_.md), чтобы преобразовать ваш запрос в SQL-строку, подходящую для [диалекта SQL](03_sql_dialect_.md) этой базы. Затем, скомпилированный SQL отправляется в низкоуровневое соединение для выполнения.

Такой слоеный подход делает `sqeleton` очень мощным: вы получаете единый интерфейс на Python, а `sqeleton` заботится о различиях между различными SQL-базами данных.

## Заключение

В этой главе вы узнали, что `sqeleton.connect()` — это ваш шлюз к взаимодействию с различными SQL-базами данных. Он упрощает процесс, скрывая сложные детали низкоуровнего соединения, позволяя использовать единый API для PostgreSQL, MySQL или облачных баз Snowflake. Также вы увидели, как отправить свой первый SQL-запрос с помощью `db.query()` и получили базовое понимание внутренней работы `sqeleton` по управлению этими соединениями.

В следующей главе мы подробно разберем, как `sqeleton` помогает вам создавать SQL-запросы программно и безопасно, без прямого написания сырых SQL-строк. Именно здесь начинается магия мощного конструктора запросов `sqeleton`!

[Следующая глава: Конструктор SQL-запросов (AST)](02_sql_query_builder__ast__.md)
