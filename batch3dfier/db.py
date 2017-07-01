"""
/***************************************************************************
 batch3dfier
 
        begin                : 2017-06-20
        copyright            : (C) 2017 by Balázs Dukai, TU Delft
        email                : balazs.dukai@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 3 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""
import psycopg2
from psycopg2 import sql


class db(object):
    """A database connection class """
    def __init__(self, dbname, host, port, user, password):
        try:
            self.conn = psycopg2.connect("dbname=%s host=%s port=%s \
                                          user=%s password=%s" \
                                          % (dbname, host, port, user, password))
            print("Opened database successfully")
        except:
            print("I'm unable to connect to the database. Exiting function.")

    def sendQuery(self, query):
        """Send a query to the DB when no results need to return (e.g. CREATE)

        Parameters
        ----------
        query : str
            

        Returns
        -------
        nothing

        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                
    
    def getQuery(self, query):
        """DB query where the results need to return (e.g. SELECT)

        Parameters
        ----------
        query : str
            SQL query
            

        Returns
        -------
        psycopg2 resultset

        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return(cur.fetchall())
        

    def vacuum(self, schema, table):
        """Vacuum analyze a table

        Parameters
        ----------
        schema : str
            schema name
        table : str
            table name

        Returns
        -------
        nothing
        """
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_‌​LEVEL_AUTOCOMMIT)
        schema = sql.Identifier(schema)
        table = sql.Identifier(table)
        query = sql.SQL("""
        VACUUM ANALYZE {schema}.{table};
        """).format(schema=schema, table=table)
        self.sendQuery(query)
    
    def close(self):
        """ """
        self.conn.close()
