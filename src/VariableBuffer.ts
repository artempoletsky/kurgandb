import _ from "lodash";

type Options = {
  safe?: boolean;
  growth?: number;
}



type IBuffer = Pick<Buffer, "writeDoubleLE"
  | "readDoubleLE"
  | "writeUint8"
  | "readUint8"
  | "writeUint16LE"
  | "readUint16LE"
  | "writeUint32LE"
  | "readUint32LE"
  | "copy"
>;


const DEFAULT_OPTIONS: Required<Options> = {
  safe: false,
  growth: 1024 * 1024,
};

export default class VariableBuffer implements IBuffer {
  protected options: Required<Options>;
  protected buffers: Buffer[];
  constructor(startingSize: number, options?: Options) {
    this.options = _.defaults({}, options, DEFAULT_OPTIONS);
    this.buffers = [];
    this.grow(startingSize);
  }

  protected grow(size?: number) {
    const { growth, safe } = this.options;
    if (!size) {
      size = growth;
    }
    this.buffers.push(safe ? Buffer.alloc(size) : Buffer.allocUnsafe(size));
  }

  protected mapAddr(position?: number): { bufferIndex: number; position: number; } {
    if (!position) return {
      bufferIndex: 0,
      position: 0,
    };
    let currentBufferIndex = 0;
    let currentMaxPos = this.buffers[0].byteLength;
    while (position > currentMaxPos) {
      position -= currentMaxPos;
      currentBufferIndex++;
      if (currentBufferIndex >= this.buffers.length) {
        this.grow();
      }
      currentMaxPos = this.buffers[currentBufferIndex].byteLength;
    }
    return {
      bufferIndex: currentBufferIndex,
      position,
    }
  }

  writeDoubleLE(value: number, offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].writeDoubleLE(value, position);
  }
  readDoubleLE(offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].readDoubleLE(position);
  }
  writeUint8(value: number, offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].writeUint8(value, position);
  }
  readUint8(offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].readUint8(position);
  }
  writeUint16LE(value: number, offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].writeDoubleLE(value, position);
  }
  readUint16LE(offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].readUint16LE(position);
  }
  writeUint32LE(value: number, offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].writeUint32LE(value, position);
  }
  readUint32LE(offset?: number): number {
    const { bufferIndex, position } = this.mapAddr(offset);
    return this.buffers[bufferIndex].readUint32LE(position);
  }

  copy(target: Uint8Array, targetStart?: number, sourceStart?: number, sourceEnd?: number): number {
    const { bufferIndex: startBuffer, position: startBufferStartPos } = this.mapAddr(sourceStart);
    const { bufferIndex: endBuffer, position: endBufferEndPos } = this.mapAddr(sourceStart);
  
    if (startBuffer == endBuffer) {
      return this.buffers[startBuffer].copy(target, targetStart, startBufferStartPos, endBufferEndPos);
    }

    let result = 0;

    let writingPos = targetStart || 0;
    for (let i = startBuffer; i <= endBuffer; i++) {
      const b = this.buffers[i];
      let start = i == startBuffer ? startBufferStartPos : 0;
      let end = i == endBuffer ? endBufferEndPos : b.byteLength;

      let copied = b.copy(target, writingPos, start, end);
      result += copied;
      writingPos += copied;
    }
    
    return result;
  }
}