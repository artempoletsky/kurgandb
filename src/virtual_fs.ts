import fs, { readFileSync, renameSync, rmSync, statSync, writeFileSync } from "fs";

import _, { debounce } from "lodash";
import { rimraf } from "rimraf";

const CWD = process.cwd();

let ROOT = CWD;

export function setRootDirectory(newRoot: string) {
  if (newRoot.endsWith("/")) {
    newRoot.slice(0, -1);
  }
  ROOT = newRoot;
}

const CACHE: Record<string, VirtualFile> = {};


export function readFile(relativePath: string): any {
  return openFile(relativePath).read();
}


export function writeFile(relativePath: string, data: any) {
  return openFile(relativePath).write(data);
}

export function removeFile(relativePath: string) {
  openFile(relativePath).remove();
}

export function renameFile(oldRelativePath: string, newRelativePath: string) {
  const file = openFile(oldRelativePath);
  file.rename(newRelativePath);
}

export function exists(relativePath: string) {
  return openFile(relativePath).exists;
}

export function renameDir(oldRelativePath: string, newRelativePath: string) {
  renameSync(ROOT + oldRelativePath, ROOT + newRelativePath);
  const oldPaths = Object.keys(CACHE);
  for (const path of oldPaths) {
    if (path.startsWith(oldRelativePath)) {
      const file = CACHE[path];
      delete CACHE[path];
      const newPath = newRelativePath + path.slice(oldRelativePath.length);
      CACHE[newPath] = file;
      file._onRenameDir(newPath);
    }
  }
}

export function rmie(name: string) {
  rimraf.sync(`${ROOT}/${name}`);
}

export function existsSync(filename: string) {
  return fs.existsSync(`${ROOT}/${filename}`);
}

export function mkdirSync(name: string) {
  fs.mkdirSync(`${ROOT}/${name}`, { recursive: true });
}

export function trackTransaction(): string {
  const id = _.uniqueId("virtualFSTrack");
  for (const name in CACHE) {
    const file = CACHE[name];
    file.forceSave();
  }
  return id;
}

export function stopTrackingTransaction(id: string) {

}

export function abortTransaction(id: string) {
  for (const name in CACHE) {
    const file = CACHE[name];
    file.abortSaving();
  }
}
// export function abortSaving(name?: string) {
//   if (!name) {
//     for (const name in CACHE) {
//       abortSaving(name);
//     }
//     return;
//   }
//   const file = CACHE[name];
//   if (file) {
//     file.abortSaving();
//   }
// }


import path from "path";
class VirtualFile {
  protected _relativePath: string;
  protected _existsFS: boolean;

  protected _savedPromise: Promise<void> | undefined;
  protected _data: any;

  protected _dataIsDirty: boolean;
  protected debouncedRemoveFromCache: () => void;

  protected onRemove: any;

  constructor(relativePath: string, defaultData?: any) {
    this._relativePath = relativePath;
    this._existsFS = fs.existsSync(ROOT + relativePath);
    this._dataIsDirty = false;

    this.debouncedRemoveFromCache = debounce(() => {
      delete CACHE[this._relativePath];
      this.forceSave();
      this._savedPromise = undefined;
      this.onRemove();
    }, 500);

    if (defaultData !== undefined && !this._existsFS) {
      this.write(defaultData);
    }
  }

  touch() {
    CACHE[this._relativePath] = this;
    if (!this._savedPromise) {
      this._savedPromise = new Promise(resolve => {
        this.onRemove = resolve;
      });
    }
    this.debouncedRemoveFromCache();
    return this._savedPromise;
  }

  public get type(): "json" {
    return "json";
  }

  public get exists(): boolean {
    return this._data !== undefined || this._existsFS;
  }

  public get relativePath(): string {
    return this._relativePath;
  }

  public get absolutePath(): string {
    return ROOT + this._relativePath;
  }

  public get isCached(): boolean {
    return !!CACHE[this._relativePath];
  }

  public get saved(): Promise<void> {
    if (!this._savedPromise) return Promise.resolve();

    return this._savedPromise;
  }

  public get isSaved(): boolean {
    return !!this._savedPromise;
  }

  read(): any {
    if (this._data !== undefined) return this._data;
    if (!this._existsFS) return undefined;
    this._data = JSON.parse(readFileSync(this.absolutePath, { encoding: "utf-8" }));
    this._dataIsDirty = false;
    this.touch();
    return this._data;
  }

  remove() {
    if (this._existsFS) {
      rmSync(this.absolutePath);
      this._existsFS = false;
    }
    delete CACHE[this._relativePath];
    this._data = undefined;
    this._dataIsDirty = false;
    // return this.touch();
  }

  write(data: any) {
    if (typeof data.toJSON == "function") {
      data = data.toJSON();
    }
    // if (data !== undefined) {
    //   data = JSON.parse(JSON.stringify(data));
    // }


    this._data = data;
    this._dataIsDirty = true;
    return this.touch();
  }

  _onRenameDir(newRelativePath: string) {
    this._relativePath = newRelativePath;
  }

  rename(newRelativePath: string) {
    if (CACHE[newRelativePath]) throw new Error(`file ${newRelativePath} already exists in the virtual file system! ${this._relativePath}`);

    if (this._existsFS) {
      if (fs.existsSync(ROOT + newRelativePath)) throw new Error(`file ${newRelativePath} already exists in the real file system!`);
      renameSync(ROOT + this._relativePath, ROOT + newRelativePath);
    }

    delete CACHE[this._relativePath];
    this._relativePath = newRelativePath;
    return this.touch();
  }

  size() {
    if (this._existsFS) {
      return statSync(this.absolutePath).size;
    }
    return 0;
  }

  forceSave() {
    if (this._dataIsDirty) {
      const data = this._data;
      if (data !== undefined) {
        const dir = path.dirname(this.absolutePath);
        if (!existsSync(dir)) {
          fs.mkdirSync(dir, {
            recursive: true
          });
        }
        fs.writeFileSync(this.absolutePath, JSON.stringify(data));
        this._existsFS = true;
      } else {
        this._existsFS = false;
        if (fs.existsSync(this.absolutePath)) {
          rmSync(this.absolutePath);
        }
      }
    }
    this._dataIsDirty = false;
  }

  abortSaving() {
    this._data = undefined;
    delete CACHE[this._relativePath];
    this._dataIsDirty = false;
  }
}

export function openFile(relativePath: string): VirtualFile {
  if (CACHE[relativePath]) return CACHE[relativePath];
  return new VirtualFile(relativePath);
}

export function allIsSaved() {
  const promises: Promise<void>[] = [];
  for (const path in CACHE) {
    const file = CACHE[path];
    promises.push(file.saved);
  }
  return Promise.all(promises);
}

export default {
  allIsSaved,
  writeFile,
  readFile,
  renameFile,
  setRootDirectory,
  removeFile,
  openFile,
  renameDir,
}

