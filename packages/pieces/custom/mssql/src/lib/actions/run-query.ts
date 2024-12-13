import { createAction, Property } from '@activepieces/pieces-framework';
import { mssqlAuth } from '../..';
import { dbClient } from '../common';

export const runQuery = createAction({
  auth: mssqlAuth,
  name: 'run-query',
  displayName: 'Run Query',
  description: 'Run Query',
  props: {
    markdown: Property.MarkDown({
      value: `
      **DO NOT** insert dynamic input directly into the query string. Instead, use $1, $2, $3 and add them in args for parameterized queries to prevent **SQL injection.**`
    }),

    query: Property.ShortText({
      displayName: 'Query',
      description: 'Please use $1, $2, etc. for parameterized queries to avoid SQL injection.',
      required: true,
    }),
    args: Property.Object({
      displayName: 'Arguments',
      description: 'Arguments to be used in the query',
      required: false,
    }),
    requestTimeout: Property.Number({
      displayName: 'Query Timeout',
      description:
        'An integer indicating the maximum number of milliseconds to wait for a query to complete before timing out.',
      required: false,
      defaultValue: 30000,
    })
  },
  async run(context) {
    const client = await dbClient(context.auth);
    const { query } = context.propsValue;
    const args = context.propsValue.args || {}
    const request = client.request();
    Object.keys(args).forEach((key, index) => {
      request.input(key, args[key]);
    })

    try {
      const result = await request.query(query);
      return result.recordset;
    } finally {
      await client.close();
    }
  },
});
