@0x8fae626c0af067f5;

struct Payload {
    bits @0 :UInt8;
    bytes @1 :Data;
}

struct Child {
    offset @0 :UInt64;  
    transition @1 :UInt8;
}

struct Node {
    payload @0 :Payload;
    children @1 :List(Child);
}
