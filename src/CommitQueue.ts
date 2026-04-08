

export default class CommitQueue {
  private static queue: Set<string> = new Set();
  private static lastId: number = 0;
  private static callbacks: Set<() => void> = new Set();

  static register(prefix: string): string {
    this.lastId++;
    let id = prefix + this.lastId;
    return id;
  }

  static async start(id: string) {
    this.crashIfBusyWith(id);
    this.queue.add(id);
  }

  static end(id: string) {
    setTimeout(() => {
      this.queue.delete(id);
      if (this.queue.size <= 0) {
        for (const fn of this.callbacks) {
          fn();
        }
        this.callbacks.clear();
      }
    });
  }

  static busy(): boolean {
    return this.queue.size > 0;
  }

  static crashIfBusyWith(id: string) {
    if (this.queue.has(id)) {
      throw "CommitQueue: trying to start a commit while another commit is in progress with id: " + id;
    }
  }

  static ready(): Promise<void> {
    if (!this.busy()) return Promise.resolve();
    return new Promise((resolve, reject) => {
      this.callbacks.add(resolve);
    });
  }
}