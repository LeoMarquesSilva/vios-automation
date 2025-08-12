export function createJobLogger(baseFn) {
  let step = 0;
  return (msg) => {
    step++;
    baseFn(`#${String(step).padStart(2,'0')} ${msg}`);
  };
}
