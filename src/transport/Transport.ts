import z, { TypeOf, ZodSchema } from "zod";
import VariableBuffer from "../VariableBuffer";

let scheme = z.object({
  str: z.string(),
  num: z.number(),
  arr: z.array(z.string()),
});

scheme.shape.str._type

type SchemeType = z.infer<typeof scheme>;

type ZByteView<T> = T & {};

let zByteView = createZByteView(scheme);

function createZByteView<T>(scheme: ZodSchema<T>): ZByteView<T> {
  let b = new VariableBuffer(0x200);
  scheme.shape
  return new Proxy(b as any, {
    get(target, key) {

    },
    set(target, key, value) {

    }
  })
}

function proxyArray(scheme: z.ZodArray<any>, target: VariableBuffer, offset: number) {
  let len = target.readUint16LE(offset);
  let offsets = new Uint16Array(target, offset + 2, len);
  return new Proxy(target as any, {
    get(t, key) {
      let offset = offsets[key as any];
      return target.readDoubleLE(offset);
    },
    set(target, key, value) {

    }
  });
}

function serializeData(scheme: any, data: any, currentOffset: number, buffer: VariableBuffer) {
  if (scheme.type == "string") {
    let len: number = Buffer.byteLength(data);
    buffer.writeUint32LE(len, currentOffset);
    currentOffset += 4;
    buffer.write(data, currentOffset);
    currentOffset += len;
  } else if (scheme.type == "number") {
    buffer.writeDoubleLE(data, currentOffset);
    currentOffset += data.length;
  } else if (scheme.type == "array") {

  }
}