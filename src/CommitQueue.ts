

export default class CommitQueue {
  private static queue: Set<string> = new Set();
  private static lastId: number = 0;
  private static callbacks: Set<() => void> = new Set();

  static register(prefix: string): string {
    this.lastId++;
    let id = prefix + this.lastId;
    return id;
  }

  static currentId: string = "";

  static start(id: string) {
    if (this.currentId) {
      throw new Error(`CommitQueue: trying to start a commit: "${id}" while another commit is in progress with id: "${this.currentId}"`);
    }
    // this.crashIfBusyWith(id);
    // this.queue.add(id);
    this.currentId = id;
  }

  static end(id: string) {
    if (this.currentId != id)
      throw new Error(`CommitQueue: trying to end a commit: "${id}" while another commit is in progress with id: "${this.currentId}"`)
    this.currentId = "";
    setTimeout(() => {
      if (this.currentId != "") return;
      // this.queue.delete(id);
      // if (this.queue.size <= 0) {
      for (const fn of this.callbacks) {
        fn();
      }
      this.callbacks.clear();
      // }
    });
  }

  static busy(): boolean {
    return this.currentId != "";
  }

  static crashIfBusyWith(id: string) {
    if (this.currentId) {
      throw new Error(`CommitQueue: trying to start a commit: "${id}" while another commit is in progress with id: "${this.currentId}"`);
    }
  }

  static ready(): Promise<void> {
    if (!this.busy()) return Promise.resolve();
    return new Promise((resolve, reject) => {
      this.callbacks.add(resolve);
    });
  }
}