
import { DedupeStrategy, Polling, pollingHelper } from '@activepieces/pieces-common';
import { createTrigger, PiecePropValueSchema, Property, TriggerStrategy } from '@activepieces/pieces-framework';
import crypto from 'crypto';
import dayjs from 'dayjs';
import mssql from 'mssql';
import { mssqlAuth } from '../..';
import { dbClient } from '../common';

type OrderDirection = 'ASC' | 'DESC';
const polling: Polling<PiecePropValueSchema<typeof mssqlAuth>, {
    table: {
        table_schema: string,
        table_name: string
    }, order_by: string, order_direction: OrderDirection | undefined
}> = {
    strategy: DedupeStrategy.LAST_ITEM,
    items: async ({ auth, propsValue, lastItemId }) => {
        const client = await dbClient(auth)
        try {
            const lastItem = lastItemId as string;
            const [order_by, data_type] = propsValue.order_by.split('|');
            console.error('order_by ', order_by)
            console.error('data_type ', data_type)
            const result = await runQuery(client, { table: propsValue.table, order_by, lastItem: lastItem, order_direction: propsValue.order_direction })
            const items = result.recordset.map(function (row) {
                const rowHash = crypto.createHash('md5').update(JSON.stringify(row)).digest('hex');
                console.error('row[order_by] ', row[order_by])
                console.error('dayjs(row[order_by]) ', dayjs(row[order_by]))
                console.error('dayjs(row[order_by]).isValid() ', dayjs(row[order_by]).isValid())
                const timeTypes = ['date',
                    'time',
                    'datetime2',
                    'datetimeoffset',
                    'datetime',
                    'smalldatetime']
                const isTimestamp = timeTypes.includes(data_type);
                console.error('isTimestamp', isTimestamp)
                const orderValue = isTimestamp ? dayjs(row[order_by]).toISOString() : row[order_by];
                console.error('orderValue', orderValue)

                return {
                    id: orderValue + '|' + rowHash,
                    data: row,
                }
            });

            return items;
        } finally {
            await client.close();
        }
    }
};

function runQuery(client: mssql.ConnectionPool, { table, order_by, lastItem, order_direction }: { table: { table_name: string, table_schema: string }, order_by: string, order_direction: OrderDirection | undefined, lastItem: string }) {
    const lastOrderKey = (lastItem ? lastItem.split('|')[0] : null);
    const request = client.request();
    if (lastOrderKey === null) {
        console.error('lastOrderKey is null', `SELECT * FROM ${table.table_schema}.${table.table_name} ORDER BY ${order_by} ${order_direction} OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY`)
        return request.query(`SELECT * FROM ${table.table_schema}.${table.table_name} ORDER BY ${order_by} ${order_direction} OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY`)
    } else {
        switch (order_direction) {
            case 'ASC':
                console.error('ASC query', `SELECT * FROM ${table.table_schema}.${table.table_name} WHERE ${order_by} <= '${lastOrderKey}' ORDER BY ${order_by} ${order_direction}`)
                return request.query(`SELECT * FROM ${table.table_schema}.${table.table_name} WHERE ${order_by} <= '${lastOrderKey}' ORDER BY ${order_by} ${order_direction}`)
            case 'DESC':
                console.error('DESC query', `SELECT * FROM ${table.table_schema}.${table.table_name} WHERE ${order_by} >= '${lastOrderKey}' ORDER BY ${order_by} ${order_direction}`)
                return request.query(`SELECT * FROM ${table.table_schema}.${table.table_name} WHERE ${order_by} >= '${lastOrderKey}' ORDER BY ${order_by} ${order_direction}`)

            default:
                throw new Error(JSON.stringify({
                    message: 'Invalid order direction',
                    order_direction: order_direction,
                }));
        }
    }
}

export const newRow = createTrigger({
    name: 'new-row',
    auth: mssqlAuth,
    displayName: 'New Row',
    description: 'triggered when a new row is added',
    props: {
        description: Property.MarkDown({
            value: `**NOTE:** The trigger fetches the latest rows using the provided order by column (newest first), and then will keep polling until the previous last row is reached.`,
        }),
        table: Property.Dropdown({
            displayName: 'Table name',
            required: true,
            refreshers: ['auth'],
            refreshOnSearch: false,
            options: async ({ auth }) => {
                if (!auth) {
                    return {
                        disabled: true,
                        options: [],
                        placeholder: 'Please authenticate first',
                    };
                }
                const authProps = auth as PiecePropValueSchema<typeof mssqlAuth>;
                const client = await dbClient(authProps)
                try {
                    const result = await client.query(
                        `SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'`
                    );
                    const options = result.recordset.map(row => ({
                        label: `${row.table_schema}.${row.table_name}`,
                        value: {
                            table_schema: row.table_schema,
                            table_name: row.table_name,
                        },
                    }));
                    return {
                        disabled: false,
                        options,
                    };
                } finally {
                    await client.close();
                }
            }
        }),
        order_by: Property.Dropdown({
            displayName: 'Column to order by',
            description: 'Use something like a created timestamp or an auto-incrementing ID.',
            required: true,
            refreshers: ['table'],
            refreshOnSearch: false,
            options: async ({ auth, table }) => {
                if (!auth) {
                    return {
                        disabled: true,
                        options: [],
                        placeholder: 'Please authenticate first',
                    };
                }
                if (!table) {
                    return {
                        disabled: true,
                        options: [],
                        placeholder: 'Please select a table',
                    };
                }
                const authProps = auth as PiecePropValueSchema<typeof mssqlAuth>;
                const client = await dbClient(authProps)
                try {
                    const { table_name, table_schema } = table as { table_schema: string, table_name: string };
                    const query = `
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = @table_schema
                    AND table_name = @table_name
                `;
                    const params = {
                        table_schema,
                        table_name
                    }
                    const result = await client.request()
                        .input('table_schema', mssql.VarChar, params.table_schema)
                        .input('table_name', mssql.VarChar, params.table_name)
                        .query(query);

                    const options = result.recordset.map(f => {
                        return {
                            label: f.column_name,
                            value: `${f.column_name}|${f.data_type}`,
                        };
                    })
                    return {
                        disabled: false,
                        options,
                    };
                } finally {
                    await client.close();
                }
            }
        }),
        order_direction: Property.StaticDropdown<OrderDirection>({
            displayName: 'Order Direction',
            description: 'The direction to sort by such that the newest rows are fetched first.',
            required: true,
            options: {
                options: [
                    {
                        label: 'Ascending',
                        value: 'ASC',
                    },
                    {
                        label: 'Descending',
                        value: 'DESC',
                    },
                ]
            },
            defaultValue: 'DESC',
        }),
    },
    sampleData: {},
    type: TriggerStrategy.POLLING,
    async test(context) {
        return await pollingHelper.test(polling, context);
    },
    async onEnable(context) {
        const { store, auth, propsValue } = context;
        await pollingHelper.onEnable(polling, { store, propsValue, auth });
    },

    async onDisable(context) {
        const { store, auth, propsValue } = context;
        await pollingHelper.onDisable(polling, { store, propsValue, auth });
    },

    async run(context) {
        return await pollingHelper.poll(polling, context);
    },
});
