export function isDynamicStep(stepKey: string) {
  return stepKey.endsWith(']');
}

export function invocationsOfPlannedDynamicStep(plannedStepKey: string, runtimeStepKeys: string[]) {
  return runtimeStepKeys.filter((k) => k.startsWith(plannedStepKey.replace('?]', '')));
}

export function dynamicKeyWithoutIndex(stepKey: string) {
  return stepKey.split('[')[0];
}

export function replacePlannedIndex(stepKey: string, stepKeyWithIndex: string) {
  return stepKey.replace('[?]', stepKeyWithIndex.match(/(\[.*\])/)![1]);
}
