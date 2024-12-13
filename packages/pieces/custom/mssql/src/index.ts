import {
  PieceAuth,
  Property,
  createPiece,
} from '@activepieces/pieces-framework';
import { PieceCategory } from '@activepieces/shared';
import { runQuery } from './lib/actions/run-query';
import { newRow } from './lib/triggers/new-row';
import { dbClient } from './lib/common';

export const mssqlAuth = PieceAuth.CustomAuth({
  props: {
    server: Property.ShortText({
      displayName: 'Server',
      required: true,
      description:
        ' A string indicating the hostname of the PostgreSQL server to connect to.',
    }),
    port: Property.Number({
      displayName: 'Port',
      defaultValue: 1433,
      description:
        'An integer indicating the port of the PostgreSQL server to connect to.',
      required: true,
    }),
    user: Property.ShortText({
      displayName: 'User',
      required: true,
      description:
        'A string indicating the user to authenticate as when connecting to the PostgreSQL server.',
    }),
    password: PieceAuth.SecretText({
      displayName: 'Password',
      description:
        'A string indicating the password to use for authentication.',
      required: true,
    }),
    database: Property.ShortText({
      displayName: 'Database',
      description:
        'A string indicating the name of the database to connect to.',
      required: true,
    }),
    encrypt: Property.Checkbox({
      displayName: 'Encrypt',
      description: 'Connect to the postgres database over SSL',
      required: true,
      defaultValue: true,
    }),
    trustServerCertificate: Property.Checkbox({
      displayName: 'Trust Server Certificate',
      description:
        'Verify the server certificate against trusted CAs or a CA provided in the certificate field below. This will fail if the database server is using a self signed certificate.',
      required: true,
      defaultValue: false,
    })
  },
  required: true,
  validate: async ({ auth }) => {
    try {
      const client = await dbClient(auth);
      await client.close();
    }
    catch (e) {
      return {
        valid: false,
        error: JSON.stringify(e)
      };
    }
    return {
      valid: true,
    };
  }
});

export const postgres = createPiece({
  displayName: 'Mssql',
  description: "",
  minimumSupportedRelease: '0.30.0',
  categories: [PieceCategory.DEVELOPER_TOOLS],
  logoUrl: '',
  authors: [],
  auth: mssqlAuth,
  actions: [runQuery],
  triggers: [newRow],
});
