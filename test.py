import src.pysqlitedatabase as psd

db = psd.Database('person.db')

has_created = db.create_table('person', columns=[
    psd.SqliteEngine.column('id', psd.SqliteTypes.INTEGER, not_null=True, primary_key=True),
    psd.SqliteEngine.column('name', psd.SqliteTypes.TEXT)
])

print(has_created)
