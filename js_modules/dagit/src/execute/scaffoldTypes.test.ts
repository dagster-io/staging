import {ConfigEditorRunConfigSchemaFragment_allConfigTypes} from 'src/configeditor/types/ConfigEditorRunConfigSchemaFragment';
import {createTypeLookup, scaffoldType} from 'src/execute/scaffoldType';

const allConfigTypes: ConfigEditorRunConfigSchemaFragment_allConfigTypes[] = [
  {
    __typename: 'RegularConfigType',
    key: 'Any',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    givenName: 'Any',
  },
  {
    __typename: 'ArrayConfigType',
    key: 'Array.Shape.41de0e2d7b75524510155d0bdab8723c6feced3b',
    description: 'List of Array.Shape.41de0e2d7b75524510155d0bdab8723c6feced3b',
    isSelector: false,
    typeParamKeys: ['Shape.41de0e2d7b75524510155d0bdab8723c6feced3b'],
  },
  {
    __typename: 'RegularConfigType',
    key: 'Bool',
    description: '',
    isSelector: false,
    typeParamKeys: [],
    givenName: 'Bool',
  },
  {
    __typename: 'RegularConfigType',
    key: 'Float',
    description: '',
    isSelector: false,
    typeParamKeys: [],
    givenName: 'Float',
  },
  {
    __typename: 'RegularConfigType',
    key: 'Int',
    description: '',
    isSelector: false,
    typeParamKeys: [],
    givenName: 'Int',
  },
  {
    __typename: 'ScalarUnionConfigType',
    key: 'ScalarUnion.Bool-Selector.be5d518b39e86a43c5f2eecaf538c1f6c7711b59',
    description: null,
    isSelector: false,
    typeParamKeys: ['Bool', 'Selector.be5d518b39e86a43c5f2eecaf538c1f6c7711b59'],
    scalarTypeKey: 'Bool',
    nonScalarTypeKey: 'Selector.be5d518b39e86a43c5f2eecaf538c1f6c7711b59',
  },
  {
    __typename: 'ScalarUnionConfigType',
    key: 'ScalarUnion.Float-Selector.d00a37e3807d37c9f69cc62997c4a5f4a176e5c3',
    description: null,
    isSelector: false,
    typeParamKeys: ['Float', 'Selector.d00a37e3807d37c9f69cc62997c4a5f4a176e5c3'],
    scalarTypeKey: 'Float',
    nonScalarTypeKey: 'Selector.d00a37e3807d37c9f69cc62997c4a5f4a176e5c3',
  },
  {
    __typename: 'ScalarUnionConfigType',
    key: 'ScalarUnion.Int-Selector.a9799b971d12ace70a2d8803c883c863417d0725',
    description: null,
    isSelector: false,
    typeParamKeys: ['Int', 'Selector.a9799b971d12ace70a2d8803c883c863417d0725'],
    scalarTypeKey: 'Int',
    nonScalarTypeKey: 'Selector.a9799b971d12ace70a2d8803c883c863417d0725',
  },
  {
    __typename: 'ScalarUnionConfigType',
    key: 'ScalarUnion.String-Selector.e04723c9d9937e3ab21206435b22247cfbe58269',
    description: null,
    isSelector: false,
    typeParamKeys: ['String', 'Selector.e04723c9d9937e3ab21206435b22247cfbe58269'],
    scalarTypeKey: 'String',
    nonScalarTypeKey: 'Selector.e04723c9d9937e3ab21206435b22247cfbe58269',
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.1bfb167aea90780aa679597800c71bd8c65ed0b2',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'disabled',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.da39a3ee5e6b4b0d3255bfef95601890afd80709',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'enabled',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.da39a3ee5e6b4b0d3255bfef95601890afd80709',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.4d63da53a40bb42f96aad27d25ec8a9656d40975',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'in_process',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.ca5906d9a0377218b4ee7d940ad55957afa73d1b',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'multiprocess',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.fff3afcfe0467fefa4b97fb8f72911aeb0e8fe4e',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.a9799b971d12ace70a2d8803c883c863417d0725',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'json',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'pickle',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'value',
        description: null,
        isRequired: true,
        configTypeKey: 'Int',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.be5d518b39e86a43c5f2eecaf538c1f6c7711b59',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'json',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'pickle',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'value',
        description: null,
        isRequired: true,
        configTypeKey: 'Bool',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.d00a37e3807d37c9f69cc62997c4a5f4a176e5c3',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'json',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'pickle',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'value',
        description: null,
        isRequired: true,
        configTypeKey: 'Float',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.e04723c9d9937e3ab21206435b22247cfbe58269',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'json',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'pickle',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'value',
        description: null,
        isRequired: true,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.e52fa3afbe531d9522fae1206f3ae9d248775742',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'json',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'pickle',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.efc7a1aa788fafe8121049790c968cbf2ebc247b',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'filesystem',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.889b7348071b49700db678dab98bb0a15fd57ecd',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'in_memory',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.da39a3ee5e6b4b0d3255bfef95601890afd80709',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Selector.f2fe6dfdc60a1947a8f8e7cd377a012b47065bc4',
    description: null,
    isSelector: true,
    typeParamKeys: [],
    fields: [
      {
        name: 'json',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'pickle',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'value',
        description: null,
        isRequired: true,
        configTypeKey: 'Any',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.05a08360c910074c8cf9bc6650b3472a3d15ad06',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'config',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.da2dd84de642ea937126a29d9803892589d8ee82',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'inputs',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.cf444913b0981924cbc22518d439fba295e78350',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'outputs',
        description: null,
        isRequired: false,
        configTypeKey: 'Array.Shape.41de0e2d7b75524510155d0bdab8723c6feced3b',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.0cbad674cfd09741fb938d7d3ec6db53621f2952',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'save_metrics',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.05a08360c910074c8cf9bc6650b3472a3d15ad06',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.241ac489ffa5f718db6444bae7849fb86a62e441',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'log_level',
        description: null,
        isRequired: false,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'name',
        description: null,
        isRequired: false,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.3baab16166bacfaf4705811e64d356112fd733cb',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'config',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.241ac489ffa5f718db6444bae7849fb86a62e441',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.41de0e2d7b75524510155d0bdab8723c6feced3b',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'result',
        description: null,
        isRequired: false,
        configTypeKey: 'Selector.e52fa3afbe531d9522fae1206f3ae9d248775742',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'path',
        description: null,
        isRequired: true,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.889b7348071b49700db678dab98bb0a15fd57ecd',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'config',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.e26e0c525e2d2c66b5a06f4cfdd053de6d44e3ed',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.979b3d2fece4f3eb92e90f2ec9fb4c85efe9ea5c',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'marker_to_close',
        description: null,
        isRequired: false,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'retries',
        description: null,
        isRequired: false,
        configTypeKey: 'Selector.1bfb167aea90780aa679597800c71bd8c65ed0b2',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.a476f98f7c4e324d4b665af722d1f2cd7f99b023',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'max_concurrent',
        description: null,
        isRequired: false,
        configTypeKey: 'Int',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'retries',
        description: null,
        isRequired: false,
        configTypeKey: 'Selector.1bfb167aea90780aa679597800c71bd8c65ed0b2',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.ae323aa79f10c476463e15598f9a75caccc76e59',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'execution',
        description: null,
        isRequired: false,
        configTypeKey: 'Selector.4d63da53a40bb42f96aad27d25ec8a9656d40975',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'intermediate_storage',
        description: null,
        isRequired: false,
        configTypeKey: 'Selector.efc7a1aa788fafe8121049790c968cbf2ebc247b',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'loggers',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.ebeaf4550c200fb540f2e1f3f2110debd8c4157c',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'resources',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.da39a3ee5e6b4b0d3255bfef95601890afd80709',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'solids',
        description: null,
        isRequired: true,
        configTypeKey: 'Shape.0cbad674cfd09741fb938d7d3ec6db53621f2952',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'storage',
        description: null,
        isRequired: false,
        configTypeKey: 'Selector.efc7a1aa788fafe8121049790c968cbf2ebc247b',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.ca5906d9a0377218b4ee7d940ad55957afa73d1b',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'config',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.979b3d2fece4f3eb92e90f2ec9fb4c85efe9ea5c',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.cf444913b0981924cbc22518d439fba295e78350',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'data_path',
        description: null,
        isRequired: true,
        configTypeKey: 'Selector.f2fe6dfdc60a1947a8f8e7cd377a012b47065bc4',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.da2dd84de642ea937126a29d9803892589d8ee82',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'abc',
        description: null,
        isRequired: true,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
      {
        name: 'bcd',
        description: null,
        isRequired: true,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.da39a3ee5e6b4b0d3255bfef95601890afd80709',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.e26e0c525e2d2c66b5a06f4cfdd053de6d44e3ed',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'base_dir',
        description: null,
        isRequired: false,
        configTypeKey: 'String',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.ebeaf4550c200fb540f2e1f3f2110debd8c4157c',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'console',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.3baab16166bacfaf4705811e64d356112fd733cb',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'CompositeConfigType',
    key: 'Shape.fff3afcfe0467fefa4b97fb8f72911aeb0e8fe4e',
    description: null,
    isSelector: false,
    typeParamKeys: [],
    fields: [
      {
        name: 'config',
        description: null,
        isRequired: false,
        configTypeKey: 'Shape.a476f98f7c4e324d4b665af722d1f2cd7f99b023',
        __typename: 'ConfigTypeField',
      },
    ],
  },
  {
    __typename: 'RegularConfigType',
    key: 'String',
    description: '',
    isSelector: false,
    typeParamKeys: [],
    givenName: 'String',
  },
];

const typeLookup = createTypeLookup(allConfigTypes);

describe('scaffoldType', () => {
  describe('`RegularConfigType` scaffolding', () => {
    expect(scaffoldType('Any', typeLookup)).toEqual('AnyType');
    expect(scaffoldType('Array.String', typeLookup)).toEqual([]);
    expect(scaffoldType('Bool', typeLookup)).toEqual(true);
    expect(scaffoldType('String', typeLookup)).toEqual('');
    expect(scaffoldType('Int', typeLookup)).toEqual(0);
  });

  describe('`Enum` scaffolding', () => {
    expect(scaffoldType('CowboyType', typeLookup)).toEqual('good|bad|ugly');
  });

  describe('`Noneable` scaffolding', () => {
    expect(scaffoldType('IntSourceType', typeLookup)).toEqual(0);
    expect(scaffoldType('Noneable.String', typeLookup)).toEqual('');
    expect(scaffoldType('Noneable.Array.String', typeLookup)).toEqual([]);
    expect(
      scaffoldType('Noneable.Shape.edaf58ebbb1501016391c8d9f0ae1a054053279e', typeLookup),
    ).toEqual({
      inner_noneable_shape_array: [],
      inner_noneable_shape_string: '',
    });
  });

  describe('`StringSource` scaffolding', () => {
    expect(scaffoldType('StringSourceType', typeLookup)).toEqual('');
  });

  describe('`CompositeConfigType` scaffolding', () => {
    expect(scaffoldType('Shape.adb69eb5b4b22eb43c3732584a5669bfdf273a3a', typeLookup)).toEqual({
      inner_shape_array: [],
      inner_shape_string: '',
    });
    expect(scaffoldType('Permissive.adb69eb5b4b22eb43c3732584a5669bfdf273a3a', typeLookup)).toEqual(
      {
        inner_shape_array: [],
        inner_shape_string: '',
      },
    );
    expect(scaffoldType('Selector.b44d6b5f1205a535d99782cde06b346523e7053e', typeLookup)).toEqual(
      '<selector>',
    );
  });

  describe('scaffoldType recursive descent', () => {
    expect(scaffoldType('Shape.73718412397d1e8ba7ebde246c44589f77c5e312', typeLookup)).toEqual({
      solids: {
        test_solid: {
          config: {
            any: 'AnyType',
            array: [],
            boolean: true,
            complex_shape: {
              inner_shape_array: [],
              inner_shape_string: '',
            },
            enum: 'good|bad|ugly',
            int_source: 0,
            noneable_array: [],
            noneable_complex_shape: {
              inner_noneable_shape_array: [],
              inner_noneable_shape_string: '',
            },
            noneable_string: '',
            number: 0,
            permissive_complex_shape: {
              inner_shape_array: [],
              inner_shape_string: '',
            },
            selector: '<selector>',
            string: '',
            string_source: '',
          },
        },
      },
    });
  });
});
