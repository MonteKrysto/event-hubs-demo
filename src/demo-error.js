export class DemoError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.status = status;
  }
}
