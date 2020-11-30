import {Story, Meta} from '@storybook/react/types-6-0';
import faker from 'faker';
import * as React from 'react';

import {DeletionDialog, Props as DeletionDialogProps} from 'src/runs/DeletionDialog';
import {ApolloTestProvider} from 'src/testing/ApolloTestProvider';

// eslint-disable-next-line import/no-default-export
export default {
  title: 'DeletionDialog',
  component: DeletionDialog,
} as Meta;

const Template: Story<DeletionDialogProps> = (props) => (
  <ApolloTestProvider>
    <DeletionDialog {...props} />
  </ApolloTestProvider>
);

const ids = [
  faker.random.uuid().slice(0, 8),
  faker.random.uuid().slice(0, 8),
  faker.random.uuid().slice(0, 8),
];

export const Success = Template.bind({});
Success.args = {
  isOpen: true,
  onClose: () => {
    console.log('Close!');
  },
  onTerminateInstead: () => {
    console.log('Terminate instead!');
  },
  selectedIDs: [
    faker.random.uuid().slice(0, 8),
    faker.random.uuid().slice(0, 8),
    faker.random.uuid().slice(0, 8),
  ],
  terminatableIDs: [ids[0]],
};
