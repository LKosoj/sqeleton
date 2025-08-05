# Глава 6: Миксины диалектов

В [предыдущей главе](05_sql_compiler_.md) вы узнали, как [SQL-компилятор](05_sql_compiler_.md) `sqeleton` берет ваш абстрактный план запроса и, используя конкретные правила [SQL-диалекта](03_sql_dialect_.md) для вашей базы данных, преобразует его в исполняемый SQL. Это позволяет писать код на Python, который работает с множеством различных баз данных, даже если их базовый синтаксис SQL (например, использование кавычек `` `my_table` `` или `'my_table'`) отличается.

Но что делать с более сложными или специфическими функциями, реализация которых может отличаться в разных базах данных? Например, как посчитать хеш MD5, или обеспечить постоянное форматирование метки времени, или получить случайную выборку строк? Разные базы данных используют разные функции или синтаксис SQL для этих задач (например, `MD5()` в MySQL против `md5()` в PostgreSQL или сложные операции `CAST` для форматирования дат).

Представьте, что у вас есть команда шеф-поваров (ваши базы данных), и у каждого свой способ приготовления простого супа (стандартных SQL-запросов). Но что если вам нужен особенный "острый" суп? Каждый повар может иметь свой уникальный рецепт добавления остроты. **Миксин диалекта** в `sqeleton` — это как **специальная "рецептурная карточка"**, которую вы можете дать повару (диалекту). Она задает стандартный способ попросить "острый" суп, а карточка говорит *этому конкретному повару* точно, как его приготовить, используя его уникальные ингредиенты и техники.

Основная задача, которую решают **Миксины диалектов** `sqeleton`, — это **предоставление повторно используемых, межбаз данных функций для возможностей, реализуемых по-разному в SQL**. Они расширяют возможности диалекта базы данных, позволяя вам писать код на Python, который остается универсальным, а `sqeleton` занимается генерацией специфичного для базы SQL для этих специальных задач.

## Что такое миксины диалектов?

Миксины — это по сути **опциональные "наборы функций"**, которые можно добавить к диалекту базы данных. Они содержат общие, повторно используемые функции, такие как:

*   **`AbstractMixin_NormalizeValue`**: стандартизирует преобразование различных типов данных (меток времени, чисел, булевых значений, UUID) в единую строковую форму в SQL.
*   **`AbstractMixin_MD5`**: предоставляет стандартный способ вычисления хеша MD5, работающего во многих базах данных.
*   **`AbstractMixin_RandomSample`**: позволяет получать случайные выборки данных независимо от базы.
*   **`AbstractMixin_TimeTravel`**: дает возможность запрашивать исторические данные (поддерживается такими базами, как Snowflake и BigQuery).

## Использование миксинов диалектов

Вы активируете миксины с помощью метода `sqeleton.connect.load_mixins()`. Этот метод действует непосредственно на объект `connect`, создавая *новую* фабрику соединений, которая включает желаемые возможности миксинов.

Рассмотрим простой пример: нормализация булевого значения в строку `'0'` или `'1'`.

```python
from sqeleton import connect
from sqeleton.abcs.mixins import AbstractMixin_NormalizeValue
from sqeleton.abcs.database_types import Boolean

# 1. Загружаем миксин 'NormalizeValue' в нашу фабрику соединений
#    Это создает новый объект 'connect' с добавленными возможностями.
connect_with_norm = connect.load_mixins(AbstractMixin_NormalizeValue)

# 2. Подключаемся к базе данных с помощью этой новой фабрики (например, DuckDB)
db = connect_with_norm("duckdb://:memory:")

# 3. Теперь у объекта диалекта есть метод normalize_boolean из миксина
#    Он возвращает SQL-выражение для нормализации булевого значения.
norm_sql_expr = db.dialect.normalize_boolean("'TRUE'", Boolean())

print(f"SQL для нормализации булевого значения (DuckDB): {norm_sql_expr}")

db.close()
```

Вывод:
```
SQL для нормализации булевого значения (DuckDB): 'TRUE'::INTEGER::VARCHAR
```
Как видно, `db.dialect.normalize_boolean` сгенерировал конкретный SQL `('TRUE'::INTEGER::VARCHAR)`, который DuckDB использует для преобразования булевых значений в строку '0' или '1'.

### Миксины с разными реализациями для баз данных

Настоящая мощь миксинов проявляется, когда одна и та же концептуальная функция требует разного SQL для разных баз данных. Рассмотрим `normalize_timestamp`, которая обеспечивает одинаковое форматирование метки времени.

**Реализация для PostgreSQL/DuckDB:**

```python
from sqeleton import connect
from sqeleton.abcs.mixins import AbstractMixin_NormalizeValue
from sqeleton.abcs.database_types import Timestamp

connect_with_norm = connect.load_mixins(AbstractMixin_NormalizeValue)
db_pg_or_duckdb = connect_with_norm("postgresql://user:pass@localhost:5432/db") # Или "duckdb://:memory:"

# Запрос на нормализацию метки времени с точностью до 3 знаков после запятой
sql_expr_pg = db_pg_or_duckdb.dialect.normalize_timestamp(
    "'2023-01-01 12:34:56.123456'",
    Timestamp(precision=3)
)
print(f"SQL для метки времени (PostgreSQL/DuckDB) (часть): {sql_expr_pg[:60]}...")
db_pg_or_duckdb.close()
```
Вывод (обрезан для читаемости):
```
SQL для метки времени (PostgreSQL/DuckDB) (часть): rpad(substring(strftime('2023-01-01 12:34:56.123456'::times...
```
Здесь используется подход с `strftime`, `substring` и `rpad`, типичный для PostgreSQL/DuckDB.

**Реализация для BigQuery:**

Теперь посмотрим, как BigQuery реализует ту же задачу с помощью своих функций.

```python
from sqeleton import connect
from sqeleton.abcs.mixins import AbstractMixin_NormalizeValue
from sqeleton.abcs.database_types import Timestamp

connect_with_norm = connect.load_mixins(AbstractMixin_NormalizeValue)

# Для демонстрации без реального подключения к BigQuery можно смоделировать его диалект:
from sqeleton.databases.bigquery import Dialect as BigQueryDialect
class MockBigQueryDB:
    dialect = BigQueryDialect()

db_bq = MockBigQueryDB()  # Используем фиктивный объект базы данных

sql_expr_bq = db_bq.dialect.normalize_timestamp(
    "'2023-01-01 12:34:56.123456'",
    Timestamp(precision=3)
)
print(f"SQL для метки времени BigQuery (часть): {sql_expr_bq[:60]}...")
# Для мок-объекта вызов db.close() не нужен
```
Вывод (обрезан для читаемости):
```
SQL для метки времени BigQuery (часть): RPAD(LEFT(FORMAT_TIMESTAMP('%F %H:%M:%E6S', '2023-01-01 12:34:56.123456'), 60)...
```
Обратите внимание, что одинаковый Python-запрос к normalize_timestamp приводит к разному SQL, характерному для каждой базы данных. Это и есть основное преимущество миксинов: писать один раз на Python, а `sqeleton` адаптирует SQL под конкретную базу.

### Обработка неподдерживаемых миксинов

Не все базы данных поддерживают все миксины. Например, `AbstractMixin_TimeTravel` (для запросов к историческим данным) поддерживается только такими системами, как Snowflake и BigQuery. Если вы попытаетесь загрузить миксин, который не реализован для выбранной базы, `sqeleton` сообщит вам об ошибке:

```python
from sqeleton import connect
from sqeleton.abcs.mixins import AbstractMixin_TimeTravel

try:
    # Это не сработает, потому что DuckDB не реализует time_travel
    connect.load_mixins(AbstractMixin_TimeTravel)("duckdb://:memory:")
except NotImplementedError as e:
    print(f"Обнаружена ожидаемая ошибка: {e}")
except TypeError as e:
    print(f"Обнаружена ожидаемая ошибка: {e}")
```
Вывод:
```
Обнаружена ожидаемая ошибка: Can't instantiate abstract class DuckDBDialect with abstract method time_travel
```
Чтобы этого избежать, можно использовать `connect.for_databases()`, чтобы задать список поддерживаемых типов баз данных для вашей расширенной фабрики соединений:

```python
from sqeleton import connect
from sqeleton.abcs.mixins import AbstractMixin_TimeTravel

# Создаем фабрику соединений, которая работает только с BigQuery и Snowflake
# и включает миксин AbstractMixin_TimeTravel
connect_for_cloud = connect.for_databases('bigquery', 'snowflake').load_mixins(AbstractMixin_TimeTravel)

# Теперь эта фабрика успешно создаст соединение с BigQuery (при наличии настроек)
# db_bq = connect_for_cloud("bigquery://мой-проект/мой-набор-данных")
# print("Успешное создание соединения BigQuery с миксином TimeTravel!")
# db_bq.close()

# А попытка подключиться к DuckDB вызовет ошибку
try:
    connect_for_cloud("duckdb://:memory:")
except NotImplementedError as e:
    print(f"Обнаружена ожидаемая ошибка (DuckDB не поддерживается этой фабрикой): {e}")
except Exception as e:
    print(f"Обнаружена неожиданная ошибка: {e}")
```
Результат:
```
Обнаружена неожиданная ошибка: DuckDB is not configured for connection factory
```
Это связано с тем, что `for_databases` ограничивает типы баз данных, которые может создавать ваша расширенная фабрика.

## Что происходит "под капотом": как работают миксины

Когда вы вызываете `connect.load_mixins()`, `sqeleton` не просто "добавляет" методы к существующим объектам диалектов. Вместо этого он динамически создает *новые* классы диалектов, объединяющие базовые возможности с функциями миксинов.

### Общий принцип загрузки миксинов (схема)

```mermaid
sequenceDiagram
    participant ВашКод as Ваш Python-код
    participant GlobalConnect as sqeleton.connect
    participant MixinRegistry as Внутренний реестр миксинов
    participant НовыйДиалект as Новый класс диалекта
    participant ОбъектБД as Объект базы данных

    ВашКод->>GlobalConnect: Вызов connect.load_mixins(AbstractMixin_X)
    GlobalConnect->>GlobalConnect: Перебирает все известные базовые диалекты
    loop Для каждого базового диалекта (например, PostgresqlDialect)
        GlobalConnect->>MixinRegistry: Проверяет, реализует ли базовый диалект AbstractMixin_X
        MixinRegistry-->>GlobalConnect: Да/Нет
        alt Если реализует
            GlobalConnect->>НовыйДиалект: Динамическое создание нового класса диалекта <br/> (Базовый диалект + реализация AbstractMixin_X)
            НовыйДиалект-->>GlobalConnect: Возвращает новый класс диалекта
        else Если не реализует
            GlobalConnect->>GlobalConnect: Помечает как недоступный для подключения <br/> (или вызывает ошибку)
        end
    end
    GlobalConnect-->>ВашКод: Возвращает новую фабрику соединений
    ВашКод->>GlobalConnect: Вызов new_connect_factory("duckdb://...")
    GlobalConnect->>ОбъектБД: Создает объект базы данных <br/> с новым диалектом
    ОбъектБД-->>ВашКод: Возвращает объект базы данных с возможностями миксинов
```

По сути, `load_mixins` создает набор *новых, расширенных классов диалектов*, основываясь на запрошенных миксинах. Когда вы затем подключаетесь через эту новую фабрику, создается база данных с одним из этих расширенных диалектов.

### Глубже: как реализован механизм

Механизм включает определения абстрактных миксинов, систему их загрузки в диалекты и конкретные реализации внутри каждого диалекта базы данных.

1.  **Определения абстрактных миксинов (`sqeleton/abcs/mixins.py`)**:
    В этих файлах задается "интерфейс" для каждого миксина. Они содержат абстрактные методы, которые должны реализовать конкретные диалекты.

    ```python
    # Упрощенно, из sqeleton/abcs/mixins.py

    class AbstractMixin(ABC):
        "Миксин для диалекта базы данных"

    class AbstractMixin_NormalizeValue(AbstractMixin):
        @abstractmethod
        def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
            pass

        @abstractmethod
        def normalize_number(self, value: str, coltype: FractionalType) -> str:
            pass

        def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
            """Создает SQL-выражение, преобразующее 'value' в '0' или '1'."""
            return self.to_string(value)
    ```
    Обратите внимание, что `normalize_boolean` имеет реализацию по умолчанию, поэтому диалекты могут ее переопределять при необходимости.

2.  **Загрузка миксинов в диалект (`sqeleton/databases/base.py`)**:
    Класс `BaseDialect` предоставляет метод `load_mixins`, который динамически создает новые классы диалектов, объединяющие базовые возможности с функциями миксинов.

    ```python
    # Упрощенно, из sqeleton/databases/base.py

    class BaseDialect(AbstractDialect):
        # ... другие свойства
        MIXINS = frozenset()

        @classmethod
        def load_mixins(cls, *abstract_mixins) -> "Self":
            # Отбирает реализуемые миксины, которые есть у этого диалекта
            mixins_to_load = {m for m in cls.MIXINS if issubclass(m, abstract_mixins)}

            # Создает новый класс, наследующий от базового и выбранных миксинов
            class _DialectWithMixins(cls, *mixins_to_load, *abstract_mixins):
                pass
            _DialectWithMixins.__name__ = cls.__name__
            return _DialectWithMixins()
    ```
    Этот метод ищет конкретные реализации миксинов (`cls.MIXINS`), которые соответствуют запрошенным абстрактным миксинам, и создает на лету новый класс, объединяющий их. Этот класс и есть тот, что используется при подключении.

3.  **Конкретные реализации миксинов (например, `sqeleton/databases/duckdb.py`, `sqeleton/databases/bigquery.py`)**:
    В файлах каждого конкретного диалекта базы данных реализуются конкретные классы, наследующие от абстрактных миксинов, и реализующие их методы.

    Например, для DuckDB:

    ```python
    # В sqeleton/databases/duckdb.py

    class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
        def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
            return f"rpad(substring(strftime({value}::timestamp, '%Y-%m-%d %H:%M:%S.%f'),1,{TIMESTAMP_PRECISION_POS+coltype.precision}),26,'0')"

        def normalize_number(self, value: str, coltype: FractionalType) -> str:
            return self.to_string(f"{value}::DECIMAL(38, {coltype.precision})")
        # normalize_boolean использует реализацию по умолчанию
    ```
    Для BigQuery:

    ```python
    # В sqeleton/databases/bigquery.py

    class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
        def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
            return f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {value})"

        def normalize_number(self, value: str, coltype: FractionalType) -> str:
            return f"format('%.{coltype.precision}f', {value})"

        def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
            return self.to_string(f"cast({value} as int)")
    ```
    Обе реализации предоставляют свои собственные методы для преобразования метки времени, чисел и булевых значений. При вызове `load_mixins(AbstractMixin_NormalizeValue)` для соединения с DuckDB подключится реализация из `duckdb.py`, а для BigQuery — из `bigquery.py`.

Эта система позволяет `sqeleton` расширять функциональность для различных баз данных, сохраняя при этом единый интерфейс на Python и гибко адаптируя SQL под конкретную систему.

## Итог

В этой главе вы узнали о **миксинах диалектов** — мощном механизме `sqeleton` для добавления опциональных "наборов функций" к базам данных. Вы поняли, как миксины предоставляют общие возможности (например, хеширование MD5 или нормализацию метки времени), которые в разных базах реализуются по-разному. Используя `connect.load_mixins()`, можно расширить возможности `sqeleton`, сохраняя ваш Python-код универсальным, а SQL — идеально адаптированным под выбранную базу. Также вы увидели, как `sqeleton` динамически применяет эти миксины для создания мощных, настраиваемых диалектных поведений.
