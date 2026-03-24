import fs from "fs";
import loader from "@assemblyscript/loader";

const wasmModule: any = loader.instantiateSync(fs.readFileSync("./wasmbuild/release.wasm"), {}).exports;


export let add = wasmModule.add;

export function padString(str: string) {
    return new TextEncoder().encode(str.padEnd(16, "\0"));
}

export function compareStrings(str1: string, str2: string): number {
    return wasmModule.compareStrings(str1, str2, 2);
}