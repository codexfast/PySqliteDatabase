import src.pysqlitedatabase as psd

db = psd.Database('person.db')

db.create_table('car', columns=[
    psd.SqliteEngine.column(
        'id',
        psd.SqliteTypes.INTEGER,
        not_null=True,
        primary_key=True,
    ),
    psd.SqliteEngine.column('name', psd.SqliteTypes.TEXT),
    psd.SqliteEngine.column('year', psd.SqliteTypes.INTEGER),
])

print(db.get_tables())
