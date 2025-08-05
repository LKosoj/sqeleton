# Глава 5: SQL-компилятор

В [предыдущей главе](04_database_types_and_schema_.md) вы узнали, как `sqeleton` понимает структуру и типы ваших таблиц базы данных через систему **Типы базы данных и схема**. Также вы знаете, что [Конструктор SQL-запросов (AST)](02_sql_query_builder__ast__.md) помогает вам создавать «проект запроса» с помощью объектов Python, а [SQL диалект](03_sql_dialect_.md) обеспечивает адаптацию этого проекта к разным базам данных.

Теперь представьте, что у вас есть идеально спроектированный план дома. У вас также есть все конкретные строительные нормы и материалы для вашего региона. Какой следующий шаг? Вам нужен опытный плотник или строитель, который реально *построит* дом, переводя эти планы и правила в реальную, осязаемую структуру.

Именно это делает **SQL-компилятор** `sqeleton`. Это финальный, ключевой компонент, который берет ваш абстрактный «план запроса» (AST) и, применяя конкретные «правила грамматики» [SQL диалекта](03_sql_dialect_.md) для вашей подключенной базы данных, создает фактическую строку SQL-запроса. Он переводит созданные вами объекты Python в конкретную последовательность слов и символов SQL, которые ваша база данных сможет понять и выполнить. Это — главный переводчик, который гарантирует, что все соберется правильно, прежде чем запрос отправить.

Основная задача `sqeleton` здесь — **преобразование абстрактных определений запросов в исполняемый SQL**. Он преодолевает разрыв между вашим Python-кодом и базой данных, автоматически обрабатывая все нюансы синтаксиса.

## Как работает SQL-компилятор

Обычно вы напрямую не взаимодействуете с SQL-компилятором. Когда вы вызываете `db.query(your_query_blueprint)`, `sqeleton` автоматически использует правильный компилятор для вашего соединения с базой данных. Однако `sqeleton` позволяет *явно* использовать компилятор, чтобы увидеть сырую SQL-строку, которая будет отправлена базе. Это очень удобно для отладки или понимания того, что именно `sqeleton` делает за кулисами.

Давайте возьмем наш знакомый пример таблицы `users` из [Главы 2: Конструктор SQL-запросов (AST)](02_sql_query_builder__ast__.md) и посмотрим, какой SQL генерирует компилятор.

Сначала создадим простой проект запроса:

```python
from sqeleton import table, this, connect

# 1. Определяем соединение с базой данных
#    Используем встроенную DuckDB для простоты демонстрации
db = connect("duckdb://:memory:")

# Необязательно: создаем фиктивную таблицу для контекста, хотя для компиляции это не обязательно
db.query("""
    CREATE TABLE users (
        id INTEGER,
        name VARCHAR,
        age INTEGER
    );
""")

# 2. Строим проект запроса (AST)
users_table = table('users')
query_blueprint = (
    users_table
    .select(this.id, this.name)
    .where(this.age > 25)
    .order_by(this.name)
)

print(f"Тип объекта blueprint запроса: {type(query_blueprint)}")
# Вывод: Тип объекта blueprint запроса: <class 'sqeleton.queries.ast_classes.Select'>
```
`query_blueprint` — это объект `Select`, являющийся узлом AST. Это — входные данные для SQL-компилятора.

Теперь явно попросим `sqeleton` с помощью компилятора преобразовать этот blueprint в строку SQL:

```python
from sqeleton.queries import Compiler

# Создаем экземпляр компилятора для нашего соединения
# Компилятору нужно знать, с какой базой данных он работает, чтобы выбрать диалект.
sql_compiler = Compiler(db)

# Используем compile_with_args, чтобы получить SQL-код и его параметры
compiled_code_obj = sql_compiler.compile_with_args(query_blueprint)

# Результат — объект CompiledCode с .code (строкой SQL) и .args (параметрами)
print(f"Сгенерированный SQL: {compiled_code_obj.code}")
print(f"Параметры SQL: {compiled_code_obj.args}")

db.close()  # Не забудьте закрыть соединение!
```

Вывод (для DuckDB или аналогично для PostgreSQL и др.):
```
Сгенерированный SQL: SELECT "id", "name" FROM "users" WHERE ("age" > ?) ORDER BY "name"
Параметры SQL: (25,)
```

Обратите внимание на важные моменты:
* Объект `query_blueprint` (Python-объект) преобразовался в обычную строку SQL.
* Имена столбцов и таблиц (`"id"`, `"name"`, `"users"`) корректно взяты в двойные кавычки, что является стандартом для DuckDB и PostgreSQL.
* Значение `25` из условия `this.age > 25` заменено на `?` (или `%s` для других диалектов), а само число `25` вынесено в отдельный список параметров. Это — важная мера безопасности — **параметризация запросов**, которая предотвращает SQL-инъекции. Компилятор делает это автоматически!

### Специфика диалекта при компиляции

Настоящая мощь SQL-компилятора в сочетании с [SQL диалектом](03_sql_dialect_.md) проявляется при компиляции одного и того же blueprint для разных баз данных. В [Главе 3: SQL диалект](03_sql_dialect_.md) показано, что разные базы данных используют разные стили кавычек. Компилятор использует диалект для применения этих правил.

Рассмотрим сравнение компиляции оператора `SELECT` для PostgreSQL и MySQL:

```python
from sqeleton import connect, table, this
from sqeleton.queries import Compiler

# Пример для PostgreSQL
# db_pg = connect("postgresql://user:password@localhost:5432/mydatabase") # замените на вашу строку соединения
db_pg = connect("duckdb://:memory:")  # Используем DuckDB для простоты, он ведет себя как Postgres при кавычках
pg_compiler = Compiler(db_pg)
pg_blueprint = table('my_table').select(this.my_column)
pg_sql = pg_compiler.compile_with_args(pg_blueprint).code
print(f"SQL для PostgreSQL/DuckDB: {pg_sql}")
db_pg.close()

# Пример для MySQL
# db_mysql = connect("mysql://user:password@localhost:3306/testdb") # замените на вашу строку соединения
# Для демонстрации без реальной базы данных MySQL:
# создадим фиктивный диалект, имитирующий кавычки MySQL
from sqeleton.abcs.database_types import AbstractDialect
class MockMySQLDialect(AbstractDialect):
    name = "MySQL"
    ARG_SYMBOL = "%s"
    PLACEHOLDER_TABLE = ""  # MySQL не использует FROM dual или подобное для SELECT 1
    def quote(self, s: str): return f"`{s}`"
    def timestamp_value(self, ts): return f"'{ts.isoformat()}'"
    # ... остальные методы для полного диалекта
class MockMySQLDB:  # Мок-объект базы данных
    dialect = MockMySQLDialect()
mysql_compiler = Compiler(MockMySQLDB())  # Используем мок-объект
mysql_blueprint = table('my_table').select(this.my_column)
mysql_sql = mysql_compiler.compile_with_args(mysql_blueprint).code
print(f"SQL для MySQL: {mysql_sql}")
# Нет вызова db.close(), так как объекты - фиктивные
```

Вывод:
```
SQL для PostgreSQL/DuckDB: SELECT "my_table"."my_column" FROM "my_table"
SQL для MySQL: SELECT `my_table`.`my_column` FROM `my_table`
```
Как видно, один и тот же Python-объект `table('my_table').select(this.my_column)` породил разные SQL-строки благодаря тому, что компилятор обращается к нужному [SQL диалекту](03_sql_dialect_.md).

## Внутри: процесс работы SQL-компилятора

Когда `sqeleton` компилирует blueprint запроса, он систематически обходит созданное вами дерево абстрактного синтаксиса (AST), преобразуя каждый узел в соответствующий SQL-текст.

### Общий поток работы

```mermaid
sequenceDiagram
    participant YourCode as Ваш Python код
    participant DbObj as Объект базы данных
    participant Compiler as SQL-компилятор
    participant AstNode as Узел AST (например, Select, Column)
    participant DialectObj as Объект диалекта

    YourCode->>DbObj: Вызов db.query(query_blueprint)
    DbObj->>Compiler: Создать Compiler(self)
    Compiler->>Compiler: Вызов compile_with_args(query_blueprint)
    Note over Compiler: 1. Настройка параметризации. <br/> 2. Обход дерева AST.
    loop Для каждого узла AST в blueprint
        Compiler->>AstNode: Посетить узел (например, Select, TablePath, Column)
        AstNode->>Compiler: Запросить компиляцию под-узлов (например, столбцы, выражения WHERE)
        Compiler->>DialectObj: Спросить диалект: "Как оформить этот идентификатор?" <br/> "Какой SQL для этой функции?"
        DialectObj-->>Compiler: Вернуть конкретный синтаксис SQL (например, `"name"`, `md5(?)`)
        Compiler->>Compiler: Объединить SQL-части текущего узла
    end
    Compiler-->>DbObj: Вернуть CompiledCode (финальный SQL и параметры)
    DbObj->>DbObj: Отправить SQL и параметры в реальную базу через драйвер
    DbObj-->>YourCode: Вернуть результаты запроса
```

Компилятор по сути обходит структуру дерева вашего blueprint, и для каждого элемента (имя таблицы, столбец или условие `WHERE`) спрашивает у связанного [SQL диалекта](03_sql_dialect_.md), как это представить в SQL. После этого он соединяет все фрагменты SQL в итоговый запрос.

### Погружение в код

Ядро SQL-компилятора реализовано в файле `sqeleton/queries/compiler.py`.

1.  **Класс `Compiler` (`sqeleton/queries/compiler.py`)**:
    Это основной класс, отвечающий за компиляцию. Он содержит ссылку на объект `базы данных` (а он, в свою очередь, дает доступ к `диалекту`).

    ```python
    # Упрощенно из sqeleton/queries/compiler.py
    from typing import Any, Dict, List, Optional
    from runtype import Dispatch, dataclass
    from ..abcs import AbstractCompiler, AbstractDatabase, AbstractDialect, Compilable
    from . import ast_classes as ast

    @dataclass
    class CompiledCode:  # Что возвращает компилятор
        code: str
        args: List[Any]
        type: Optional[type]

    @dataclass
    class Compiler(AbstractCompiler):
        database: AbstractDatabase
        # ... дополнительные внутренние состояния, такие как _table_context, _args ...

        @property
        def dialect(self) -> AbstractDialect:
            return self.database.dialect  # Доступ к диалекту!

        def compile_with_args(self, elem: Any, params: Optional[Dict[str, Any]] = None) -> CompiledCode:
            # Основной публичный метод
            # Позволяет получать SQL с параметрами
            # ... настройка _args_enabled ...
            res = self.compile(elem, params)  # Вызов основного метода компиляции
            # ... обработка _args в финальный список аргументов ...
            return CompiledCode(res, args, elem.type)

        def _compile(self, elem) -> str:
            # Внутренний метод, определяющий, как компилировать разные типы Python
            if isinstance(elem, Compilable):
                # Если это узел AST, делегируем конкретному методу
                return self.replace(_is_root=False).compile_node(elem)
            elif isinstance(elem, str):
                return self._add_as_param(elem)  # Обработка строк как параметров
            elif isinstance(elem, (int, float)):
                return str(elem)  # Простые типы — напрямую
            # ... остальные типы ...
            return self.dialect.timestamp_value(elem)  # Форматирование через диалект
    ```
    Метод `compile_with_args` — точка входа для получения полного SQL и его параметров. Метод `_compile` — основная рабочая часть, которая решает, как преобразовать разные Python-объекты (простые типы или узлы AST) в SQL.

2.  **Методы `compile_node` и `Dispatch` (`sqeleton/queries/compiler.py`)**:
    Обработка разных узлов AST (таких как `Select`, `Column`, `BinOp`) осуществляется с помощью `Dispatch` (из библиотеки `runtype`, псевдоним `md`). Это позволяет иметь несколько методов `compile_node`, каждый из которых специализируется на своем классе AST.

    ```python
    # В упрощении из sqeleton/queries/compiler.py
    # ...
    md = Dispatch()  # Декоратор для множественной обработки 'compile_node'

    @md
    def compile_node(self, n: ast.Select) -> str:
        # Этот метод знает, как компилировать узел 'Select'.
        # Он рекурсивно вызывает self.compile для своих под-элементов.
        columns = ", ".join(map(self.compile, n.columns)) if n.columns else "*"
        distinct = "DISTINCT " if n.distinct else ""
        select = f"SELECT {distinct}{columns}"

        if n.table:
            select += " FROM " + self.compile(n.table)  # Компиляция части таблицы
        elif self.dialect.PLACEHOLDER_TABLE:
            select += f" FROM {self.dialect.PLACEHOLDER_TABLE}"

        if n.where_exprs:
            # Компиляция каждого выражения WHERE, соединенного через " AND "
            select += " WHERE " + " AND ".join(map(self.compile, n.where_exprs))

        # ... аналогично для GROUP BY, HAVING, ORDER BY, LIMIT ...

        return select

    @md
    def compile_node(self, n: ast.Column) -> str:
        # Этот метод компилирует узел 'Column'.
        # Он использует метод quote диалекта.
        # Также он учитывает добавление алиасов таблиц, если в контексте нескольких таблиц.
        if self._table_context:  # Если мы в запросе с таблицами
            # ... логика определения, нужен ли алиас для столбца ...
            # Для простоты — прямой доступ к столбцу
            return f"{self.quote(n.source_table.name)}.{self.quote(n.name)}"  # например, "users"."id"
        return self.quote(n.name)  # например, "id"

    @md
    def compile_node(self, n: ast.BinOp) -> str:
        # Этот метод компилирует бинарные операции (например, '>', '=', '+')
        expr = f" {n.op} ".join(self.compile(a) for a in n.args)  # рекурсивно компилируем аргументы
        return f"({expr})"  # Оборачиваем в скобки для правильного порядка операций
    ```
    Каждый `compile_node` — это часть, которая отвечает за преобразование конкретного типа узла AST в SQL. Он часто вызывает `self.compile()` для своих под-элементов, обходя дерево AST. Важный момент — он использует методы диалекта (`self.dialect`), такие как `quote` или `offset_limit`, чтобы итоговый SQL соответствовал правилам конкретной базы данных.

Этот тонкий баланс между `Compiler` и `Dialect` позволяет `sqeleton` генерировать точный и корректный SQL для множества баз данных из одного, абстрактного Python-запроса.

## Итог

В этой главе вы разобрали роль **SQL-компилятора** в `sqeleton`. Вы узнали, что он — главный строитель, который берет ваши абстрактные blueprints запросов (AST) и, применяя конкретные правила [SQL диалекта](03_sql_dialect_.md), превращает их в конкретные строки SQL-запросов, понятные и исполняемые базой данных. Вы увидели, как явно скомпилировать запрос для проверки сгенерированного SQL и поняли, как компилятор обрабатывает важные аспекты — кавычки и параметризацию.

Теперь, когда мы рассмотрели основные компоненты генерации запросов `sqeleton`, включая соединение с базой, построение запросов, диалекты, схему и компилятор, мы готовы исследовать, как `sqeleton` расширяет и настраивает поведение диалектов для более сложных сценариев с помощью **Mixin-ов диалектов**.

[Следующая глава: Mixin-ы диалектов](06_dialect_mixins_.md)
