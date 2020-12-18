import {Story, Meta} from '@storybook/react/types-6-0';
import faker from 'faker';
import * as React from 'react';

import {TerminationDialog, Props as TerminationDialogProps} from 'src/runs/TerminationDialog';
import {ApolloTestProvider} from 'src/testing/ApolloTestProvider';

// eslint-disable-next-line import/no-default-export
export default {
  title: 'TerminationDialog',
  component: TerminationDialog,
} as Meta;

const Template: Story<TerminationDialogProps> = (props) => (
  <ApolloTestProvider>
    <TerminationDialog {...props} />
  </ApolloTestProvider>
);

const shared = {
  isOpen: true,
  onClose: () => {
    console.log('Close!');
  },
  selectedIDs: [
    faker.random.uuid().slice(0, 8),
    faker.random.uuid().slice(0, 8),
    faker.random.uuid().slice(0, 8),
  ],
};

export const ForceTerminationOptional = Template.bind({});
ForceTerminationOptional.args = {
  forceTerminate: 'optional',
  ...shared,
};

export const ForceTerminationAlways = Template.bind({});
ForceTerminationAlways.args = {
  forceTerminate: 'always',
  ...shared,
};

export const ForceTerminationNever = Template.bind({});
ForceTerminationNever.args = {
  forceTerminate: 'never',
  ...shared,
};
