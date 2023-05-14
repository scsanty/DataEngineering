if __name__ == "__main__":
    exit()

    docker run -d --name MSSQLServer -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=1qaz@WSX' -p 1433:1433 mcr.microsoft.com/azure-sql-edge