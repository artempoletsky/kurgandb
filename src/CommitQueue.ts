

export default class CommitQueue {
  private static queue: Set<string> = new Set();
  private static lastId: number = 0;

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
    this.queue.delete(id);
  }

  static busy(): boolean {
    return this.queue.size > 0;
  }

  static crashIfBusyWith(id: string) {
    if (this.queue.has(id)) {
      throw "CommitQueue: trying to start a commit while another commit is in progress with id: " + id;
    }
  }
}