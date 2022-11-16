import src.pysqlitedatabase as psd

db = psd.Database('person.db')
db.restore('1668635800650652900_backup_dump.sql')