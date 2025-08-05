# Глава 3: Диалект SQL

В [предыдущей главе](02_sql_query_builder__ast__.md) вы узнали, как помощник построения SQL-запросов `sqeleton` помогает создавать «эскиз запроса» (дерево абстрактного синтаксиса, или AST) с помощью объектов и методов Python. Этот эскиз — мощный, независимый от конкретной базы данных способ описания вашего SQL-запроса.

Но есть одна сложность: хотя эскиз универсален, настоящий язык SQL, на котором говорят разные базы данных, не всегда идентичен. Например, PostgreSQL может использовать один способ вычисления MD5-хеша, а MySQL — другой. Некоторые базы по-разному цитируют имена таблиц или имеют уникальные функции.

Представьте, что вы архитектор, проектирующий дом. Вы создаете детальный план, показывающий все комнаты, стены и особенности. Но при непосредственном строительстве подрядчик в одной стране может использовать другие материалы или строительные нормы, чем в другой стране, даже для одного и того же проекта.

И именно здесь на помощь приходит `sqeleton` — **диалект SQL**. Он действует как **специализированный словарь и грамматика** для конкретной базы данных. Когда `sqeleton` нужно превратить ваш универсальный эскиз запроса в реальный SQL-код, он обращается к диалекту, специфическому для вашей подключенной базы данных. Это гарантирует, что создаваемые SQL-команды идеально соответствуют точным правилам синтаксиса, функциям и особенностям именно этой базы.

Основная задача, которую решает `sqeleton`, — **генерация SQL, специфичного для базы данных**: он автоматически адаптирует ваш эскиз запроса к правильному синтаксису для PostgreSQL, MySQL, Snowflake или любой другой поддерживаемой базы, без необходимости писать разные SQL-запросы для каждой.

## Что знает диалект SQL?

Каждый `SQL диалект` в `sqeleton` — это набор правил и переводов для конкретного типа базы данных. Он знает:

*   **Как цитировать имена:** Должен ли он использовать `"my_table"` (PostgreSQL) или `` `my_table` `` (MySQL)?
*   **Конкретные имена функций:** `MD5()` или `md5()`? Требуется ли особое приведение типа для результата?
*   **Вариации синтаксиса:** Как писать конструкции `LIMIT` и `OFFSET`.
*   **Отображение типов:** Как представить общий тип «timestamp» или «десятичное число» в системе типов конкретной базы.
*   **Уникальные особенности:** Например, как обрабатывать значения null в сравнениях.

## Как `sqeleton` использует диалекты (за сценой)

Обычно вы напрямую не взаимодействуете с `SQL диалектом`. Вместо этого `sqeleton` автоматически выбирает правильный диалект при установлении соединения с [базой данных](01_database_connection_.md). Затем, когда вы используете [Построитель SQL-запросов (AST)](02_sql_query_builder__ast__.md), чтобы создать эскиз и передать его функции `db.query()`, **компилятор SQL** (`Chapter 5: SQL Compiler`, который мы рассмотрим в [главах 5](05_sql_compiler_.md)) использует этот диалект для преобразования эскиза в исполняемый SQL-строку.

Рассмотрим пример: вычисление MD5-хеша. Разные базы данных делают это немного по-разному.

```python
from sqeleton import connect, func, Literal

# 1. Подключение к базе PostgreSQL
# (замените на свою строку подключения)
db_pg = connect("postgresql://user:password@localhost:5432/mydatabase")

# 2. Создание эскиза запроса: вычислить MD5 строки 'hello'
# 'func.md5()' — универсальный способ сказать "вызвать функцию MD5"
query_blueprint_md5 = func.md5(Literal('hello'))

# В реальности вызов db.query() выполнит этот запрос.
# Для демонстрации мы *симулируем* компиляцию SQL.
# sqeleton.Compiler компилирует эскиз, используя диалект.
# (Это просто для отображения SQL; обычно делаете это не напрямую)
from sqeleton.queries.compiler import Compiler
pg_compiler = Compiler(db_pg)
pg_sql_code = pg_compiler.compile_with_args(query_blueprint_md5)

print(f"PostgreSQL SQL: {pg_sql_code.code}")
print(f"PostgreSQL args: {pg_sql_code.args}")

db_pg.close()
```

Вывод для PostgreSQL:

```
PostgreSQL SQL: md5(%s)
PostgreSQL args: ('hello',)
```

Теперь сделаем то же самое, но с подключением к MySQL:

```python
from sqeleton import connect, func, Literal
from sqeleton.queries.compiler import Compiler

# 1. Подключение к базе MySQL
# (замените на свою строку)
db_mysql = connect("mysql://user:password@localhost:3306/testdb")

# 2. Создание такого же эскиза запроса
query_blueprint_md5 = func.md5(Literal('hello'))

# Симуляция компиляции для MySQL
mysql_compiler = Compiler(db_mysql)
mysql_sql_code = mysql_compiler.compile_with_args(query_blueprint_md5)

print(f"MySQL SQL: {mysql_sql_code.code}")
print(f"MySQL args: {mysql_sql_code.args}")

db_mysql.close()
```

Вывод:

```
MySQL SQL: md5(%s)
MySQL args: ('hello',)
```

Подождите, SQL выглядит одинаково! Хотя название функции `md5()` похоже, другие аспекты сгенерированного SQL для `MD5_as_int()` (используемой для внутренних проверок) или цитирования отличались бы значительно. Например, MySQL использует обратные кавычки (`` ` ``) для цитирования идентификаторов, а PostgreSQL — двойные кавычки (`"`).

Попробуем более простой пример с цитированием таблиц и колонок.

```python
from sqeleton import connect, table, this
from sqeleton.queries.compiler import Compiler

# PostgreSQL
db_pg = connect("postgresql://user:password@localhost:5432/mydatabase")
pg_compiler = Compiler(db_pg)
pg_sql_code = pg_compiler.compile_with_args(table('my_table').select(this.my_column))
print(f"PostgreSQL SQL: {pg_sql_code.code}")
db_pg.close()

# MySQL
db_mysql = connect("mysql://user:password@localhost:3306/testdb")
mysql_compiler = Compiler(db_mysql)
mysql_sql_code = mysql_compiler.compile_with_args(table('my_table').select(this.my_column))
print(f"MySQL SQL: {mysql_sql_code.code}")
db_mysql.close()
```

Вывод:

```
PostgreSQL SQL: SELECT "my_table"."my_column" FROM "my_table"
MySQL SQL: SELECT `my_table`.`my_column` FROM `my_table`
```

Видите, как Python-выражение `table('my_table').select(this.my_column)` одинаковое, а `sqeleton` автоматически генерирует разные SQL-запросы с правильным цитированием для каждой базы! Это — мощь диалектов SQL.

## Как работает диалект SQL (за сценой)

Когда вы подключаетесь к базе данных и делаете запрос, происходит примерно так:

```mermaid
sequenceDiagram
    participant ВашКод as Ваш Python-код
    participant БД as Объект базы данных
    participant Компилятор as SQL-компилятор
    participant Диалект as Объект диалекта
    participant РеальнаяБаза as Реальная база данных

    ВашКод->>БД: Вызов db.query(эскиз_запроса)
    БД->>Компилятор: Создает экземпляр Compiler(self)
    Note over Компилятор: Компилятор получает диалект из свойства 'dialect' объекта базы данных.
    Компилятор->>Диалект: Спрашивает: "Как цитировать 'my_table'?"
    Диалект-->>Компилятор: Возвращает: '"my_table"' (для Postgres) или `` `my_table` `` (для MySQL)
    Компилятор->>Диалект: Спрашивает: "Что такое SQL для MD5(X)?"
    Диалект-->>Компилятор: Возвращает: "md5(X)" или подобное
    Компилятор->>Компилятор: Объединяет все части в финальную SQL-строку
    Компилятор-->>БД: Возвращает скомпилированный SQL
    БД->>РеальнаяБаза: Выполняет SQL
    РеальнаяБаза-->>БД: Возвращает результаты
    БД-->>ВашКод: Возвращает результаты
```

### Глубже в код

Система диалектов построена на абстрактном базовом классе (`AbstractDialect`) и множестве конкретных реализаций — по одной для каждой базы.

1.  **Базовый класс `AbstractDialect` (`sqeleton/abcs/database_types.py`)**:
    Он задает общий интерфейс для всех диалектов. В нем перечислены все методы, которые `Компилятор` может запрашивать у диалекта.

    ```python
    # Из sqeleton/abcs/database_types.py

    class AbstractDialect(ABC):
        @property
        @abstractmethod
        def name(self) -> str:
            "Имя диалекта"

        @abstractmethod
        def quote(self, s: str):
            "Цитирование SQL-имени"

        @abstractmethod
        def concat(self, items: List[str]) -> str:
            "SQL для конкатенации нескольких колонок в строку"

        @abstractmethod
        def random(self) -> str:
            "SQL для генерации случайного числа от 0 до 1"

        # ... много других абстрактных методов
    ```
    Этот класс гарантирует, что каждый диалект умеет выполнять эти стандартные SQL-операции, даже если синтаксис у них разный.

2.  **Конкретные реализации диалектов (например, `sqeleton/databases/postgresql.py`, `sqeleton/databases/mysql.py`)**:
    Для каждой базы данных есть свой класс диалекта, наследующий от `BaseDialect` (который реализует `AbstractDialect` или его части) и предоставляющий конкретные реализации методов.

    Рассмотрим, как реализован метод `quote()` для PostgreSQL и MySQL:

    ```python
    # Из sqeleton/databases/postgresql.py

    class PostgresqlDialect(BaseDialect, Mixin_Schema):
        name = "PostgreSQL"
        # ... другие свойства ...

        def quote(self, s: str):
            return f'"{s}"'  # PostgreSQL использует двойные кавычки
        # ...
    ```

    ```python
    # Из sqeleton/databases/mysql.py

    class Dialect(BaseDialect, Mixin_Schema, Mixin_OptimizerHints):
        name = "MySQL"
        # ... другие свойства ...

        def quote(self, s: str):
            return f"`{s}`"  # MySQL использует обратные кавычки
        # ...
    ```
    Как видно, метод `quote()` ведет себя по-разному в каждом диалекте, чтобы правильно формировать SQL для конкретной базы.

3.  **Свойство `dialect` объекта базы данных**:
    Каждый объект базы данных, полученный через `sqeleton.connect()`, содержит свойство `dialect`, которое — это экземпляр соответствующего класса диалекта.

    ```python
    # Из sqeleton/databases/postgresql.py (упрощенно)

    class PostgreSQL(ThreadedDatabase):
        dialect = PostgresqlDialect()  # Объект базы PostgreSQL знает свой диалект
        # ...
    ```
    Когда `Компилятор` должен сгенерировать SQL, он просто запрашивает у объекта базы данных его `dialect` и вызывает нужные методы (например, `dialect.quote()`).

Эта мощная система диалектов делает возможным, чтобы `sqeleton` предоставлял единый Python API для построения запросов, автоматически генерирующего правильный и оптимизированный SQL для различных баз данных.

## Итог

В этой главе вы узнали, что `SQL диалект` — это «языковой справочник» `sqeleton` для конкретной базы данных. Он позволяет `sqeleton` брать ваши универсальные эскизы (AST) и преобразовывать их в точный SQL-синтаксис, необходимый для таких систем, как PostgreSQL, MySQL или Snowflake, автоматически обрабатывая такие детали, как цитирование и имена функций. Эта абстракция упрощает вашу работу, позволяя сосредоточиться на *чем* вы хотите запросить, а не *как* писать запрос для каждой базы.

Далее мы изучим **Типы данных и схему базы**, чтобы понять, как `sqeleton` представляет типы данных и структуру ваших таблиц, что важно для построения надежных и правильных запросов.

[Следующая глава: Типы данных и схема базы](04_database_types_and_schema_.md)
