import sql, { config } from 'mssql';

type options = NonNullable<config["options"]>

export const dbClient = async (config: config & Pick<options, "encrypt" | "trustServerCertificate">) => {
    const { user, password, database, server, port, encrypt, trustServerCertificate, requestTimeout } = config;

    const sqlConfig = {
        user,
        password,
        database,
        server,
        port,
        options: {
            encrypt,
            trustServerCertificate
        },
        requestTimeout
    }
    const client = await sql.connect(sqlConfig);
    return client;
}
