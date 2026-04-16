import HeapDumb from "./HeapDumb";



export default class HeapLogical {
  // static dumb = HeapDumb;

  static createId(): number {

  }

  static get(id: number): Buffer {

  }

  static set(id: number, data: Buffer) {

  }

  static delete(id: number) {
    HeapDumb.free();
  }

  static getString(id: number): string {
    return this.get(id).toString("utf-8");
  }

  static getJSON(id: number): any {
    return JSON.parse(this.getString(id));
  }

  static setString(id: number, str: string) {

  }

  static setJSON(id: number, data: any) {
    this.setString(id, JSON.stringify(data));
  }
}