
import { createTrigger, TriggerStrategy, PiecePropValueSchema  } from '@activepieces/pieces-framework';
import { DedupeStrategy, Polling, pollingHelper } from '@activepieces/pieces-common';
import dayjs from 'dayjs';

// replace auth with piece auth variable
const polling: Polling< PiecePropValueSchema<typeof auth>, Record<string, never> > = {
    strategy: DedupeStrategy.TIMEBASED,
    items: async ({ propsValue, lastFetchEpochMS }) => {
        // implement the logic to fetch the items
        const items = [ {id: 1, created_date: '2021-01-01T00:00:00Z'}, {id: 2, created_date: '2021-01-01T00:00:00Z'}];
        return items.map((item) => ({
            epochMilliSeconds: dayjs(item.created_date).valueOf(),
            data: item,
            }));
        }
}

export const newRow = createTrigger({
// auth: check https://www.activepieces.com/docs/developers/piece-reference/authentication,
name: 'newRow',
displayName: 'New Row',
description: '',
props: {},
sampleData: {},
type: TriggerStrategy.POLLING,
async test(context) {
    const { store, auth, propsValue } = context;
    return await pollingHelper.test(polling, { store, auth, propsValue });
},
async onEnable(context) {
    const { store, auth, propsValue } = context;
    await pollingHelper.onEnable(polling, { store, auth, propsValue });
},

async onDisable(context) {
    const { store, auth, propsValue } = context;
    await pollingHelper.onDisable(polling, { store, auth, propsValue });
},

async run(context) {
    const { store, auth, propsValue } = context;
    return await pollingHelper.poll(polling, { store, auth, propsValue });
},
});