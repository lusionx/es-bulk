export = readlines;
declare class readlines {
    constructor(file: any, options?: any);
    fd: any;
    options: any;
    newLineCharacter: any;
    close(): void;
    next(): any;
    reset(): void;
}
