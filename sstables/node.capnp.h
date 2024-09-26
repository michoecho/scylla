// Generated by Cap'n Proto compiler, DO NOT EDIT
// source: node.capnp

#pragma once

#include <capnp/generated-header-support.h>
#include <kj/windows-sanity.h>

#ifndef CAPNP_VERSION
#error "CAPNP_VERSION is not defined, is capnp/generated-header-support.h missing?"
#elif CAPNP_VERSION != 1000002
#error "Version mismatch between generated code and library headers.  You must use the same version of the Cap'n Proto compiler and library."
#endif


CAPNP_BEGIN_HEADER

namespace capnp {
namespace schemas {

CAPNP_DECLARE_SCHEMA(94efcb88385cd556);
CAPNP_DECLARE_SCHEMA(889a1e8f2accb19d);
CAPNP_DECLARE_SCHEMA(c9717af5ce98ee86);

}  // namespace schemas
}  // namespace capnp


struct Payload {
  Payload() = delete;

  class Reader;
  class Builder;
  class Pipeline;

  struct _capnpPrivate {
    CAPNP_DECLARE_STRUCT_HEADER(94efcb88385cd556, 1, 1)
    #if !CAPNP_LITE
    static constexpr ::capnp::_::RawBrandedSchema const* brand() { return &schema->defaultBrand; }
    #endif  // !CAPNP_LITE
  };
};

struct Child {
  Child() = delete;

  class Reader;
  class Builder;
  class Pipeline;

  struct _capnpPrivate {
    CAPNP_DECLARE_STRUCT_HEADER(889a1e8f2accb19d, 2, 0)
    #if !CAPNP_LITE
    static constexpr ::capnp::_::RawBrandedSchema const* brand() { return &schema->defaultBrand; }
    #endif  // !CAPNP_LITE
  };
};

struct Node {
  Node() = delete;

  class Reader;
  class Builder;
  class Pipeline;

  struct _capnpPrivate {
    CAPNP_DECLARE_STRUCT_HEADER(c9717af5ce98ee86, 0, 2)
    #if !CAPNP_LITE
    static constexpr ::capnp::_::RawBrandedSchema const* brand() { return &schema->defaultBrand; }
    #endif  // !CAPNP_LITE
  };
};

// =======================================================================================

class Payload::Reader {
public:
  typedef Payload Reads;

  Reader() = default;
  inline explicit Reader(::capnp::_::StructReader base): _reader(base) {}

  inline ::capnp::MessageSize totalSize() const {
    return _reader.totalSize().asPublic();
  }

#if !CAPNP_LITE
  inline ::kj::StringTree toString() const {
    return ::capnp::_::structString(_reader, *_capnpPrivate::brand());
  }
#endif  // !CAPNP_LITE

  inline  ::uint8_t getBits() const;

  inline bool hasBytes() const;
  inline  ::capnp::Data::Reader getBytes() const;

private:
  ::capnp::_::StructReader _reader;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::_::PointerHelpers;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::List;
  friend class ::capnp::MessageBuilder;
  friend class ::capnp::Orphanage;
};

class Payload::Builder {
public:
  typedef Payload Builds;

  Builder() = delete;  // Deleted to discourage incorrect usage.
                       // You can explicitly initialize to nullptr instead.
  inline Builder(decltype(nullptr)) {}
  inline explicit Builder(::capnp::_::StructBuilder base): _builder(base) {}
  inline operator Reader() const { return Reader(_builder.asReader()); }
  inline Reader asReader() const { return *this; }

  inline ::capnp::MessageSize totalSize() const { return asReader().totalSize(); }
#if !CAPNP_LITE
  inline ::kj::StringTree toString() const { return asReader().toString(); }
#endif  // !CAPNP_LITE

  inline  ::uint8_t getBits();
  inline void setBits( ::uint8_t value);

  inline bool hasBytes();
  inline  ::capnp::Data::Builder getBytes();
  inline void setBytes( ::capnp::Data::Reader value);
  inline  ::capnp::Data::Builder initBytes(unsigned int size);
  inline void adoptBytes(::capnp::Orphan< ::capnp::Data>&& value);
  inline ::capnp::Orphan< ::capnp::Data> disownBytes();

private:
  ::capnp::_::StructBuilder _builder;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
  friend class ::capnp::Orphanage;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::_::PointerHelpers;
};

#if !CAPNP_LITE
class Payload::Pipeline {
public:
  typedef Payload Pipelines;

  inline Pipeline(decltype(nullptr)): _typeless(nullptr) {}
  inline explicit Pipeline(::capnp::AnyPointer::Pipeline&& typeless)
      : _typeless(kj::mv(typeless)) {}

private:
  ::capnp::AnyPointer::Pipeline _typeless;
  friend class ::capnp::PipelineHook;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
};
#endif  // !CAPNP_LITE

class Child::Reader {
public:
  typedef Child Reads;

  Reader() = default;
  inline explicit Reader(::capnp::_::StructReader base): _reader(base) {}

  inline ::capnp::MessageSize totalSize() const {
    return _reader.totalSize().asPublic();
  }

#if !CAPNP_LITE
  inline ::kj::StringTree toString() const {
    return ::capnp::_::structString(_reader, *_capnpPrivate::brand());
  }
#endif  // !CAPNP_LITE

  inline  ::uint64_t getOffset() const;

  inline  ::uint8_t getTransition() const;

private:
  ::capnp::_::StructReader _reader;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::_::PointerHelpers;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::List;
  friend class ::capnp::MessageBuilder;
  friend class ::capnp::Orphanage;
};

class Child::Builder {
public:
  typedef Child Builds;

  Builder() = delete;  // Deleted to discourage incorrect usage.
                       // You can explicitly initialize to nullptr instead.
  inline Builder(decltype(nullptr)) {}
  inline explicit Builder(::capnp::_::StructBuilder base): _builder(base) {}
  inline operator Reader() const { return Reader(_builder.asReader()); }
  inline Reader asReader() const { return *this; }

  inline ::capnp::MessageSize totalSize() const { return asReader().totalSize(); }
#if !CAPNP_LITE
  inline ::kj::StringTree toString() const { return asReader().toString(); }
#endif  // !CAPNP_LITE

  inline  ::uint64_t getOffset();
  inline void setOffset( ::uint64_t value);

  inline  ::uint8_t getTransition();
  inline void setTransition( ::uint8_t value);

private:
  ::capnp::_::StructBuilder _builder;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
  friend class ::capnp::Orphanage;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::_::PointerHelpers;
};

#if !CAPNP_LITE
class Child::Pipeline {
public:
  typedef Child Pipelines;

  inline Pipeline(decltype(nullptr)): _typeless(nullptr) {}
  inline explicit Pipeline(::capnp::AnyPointer::Pipeline&& typeless)
      : _typeless(kj::mv(typeless)) {}

private:
  ::capnp::AnyPointer::Pipeline _typeless;
  friend class ::capnp::PipelineHook;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
};
#endif  // !CAPNP_LITE

class Node::Reader {
public:
  typedef Node Reads;

  Reader() = default;
  inline explicit Reader(::capnp::_::StructReader base): _reader(base) {}

  inline ::capnp::MessageSize totalSize() const {
    return _reader.totalSize().asPublic();
  }

#if !CAPNP_LITE
  inline ::kj::StringTree toString() const {
    return ::capnp::_::structString(_reader, *_capnpPrivate::brand());
  }
#endif  // !CAPNP_LITE

  inline bool hasPayload() const;
  inline  ::Payload::Reader getPayload() const;

  inline bool hasChildren() const;
  inline  ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Reader getChildren() const;

private:
  ::capnp::_::StructReader _reader;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::_::PointerHelpers;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::List;
  friend class ::capnp::MessageBuilder;
  friend class ::capnp::Orphanage;
};

class Node::Builder {
public:
  typedef Node Builds;

  Builder() = delete;  // Deleted to discourage incorrect usage.
                       // You can explicitly initialize to nullptr instead.
  inline Builder(decltype(nullptr)) {}
  inline explicit Builder(::capnp::_::StructBuilder base): _builder(base) {}
  inline operator Reader() const { return Reader(_builder.asReader()); }
  inline Reader asReader() const { return *this; }

  inline ::capnp::MessageSize totalSize() const { return asReader().totalSize(); }
#if !CAPNP_LITE
  inline ::kj::StringTree toString() const { return asReader().toString(); }
#endif  // !CAPNP_LITE

  inline bool hasPayload();
  inline  ::Payload::Builder getPayload();
  inline void setPayload( ::Payload::Reader value);
  inline  ::Payload::Builder initPayload();
  inline void adoptPayload(::capnp::Orphan< ::Payload>&& value);
  inline ::capnp::Orphan< ::Payload> disownPayload();

  inline bool hasChildren();
  inline  ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Builder getChildren();
  inline void setChildren( ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Reader value);
  inline  ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Builder initChildren(unsigned int size);
  inline void adoptChildren(::capnp::Orphan< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>&& value);
  inline ::capnp::Orphan< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>> disownChildren();

private:
  ::capnp::_::StructBuilder _builder;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
  friend class ::capnp::Orphanage;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::_::PointerHelpers;
};

#if !CAPNP_LITE
class Node::Pipeline {
public:
  typedef Node Pipelines;

  inline Pipeline(decltype(nullptr)): _typeless(nullptr) {}
  inline explicit Pipeline(::capnp::AnyPointer::Pipeline&& typeless)
      : _typeless(kj::mv(typeless)) {}

  inline  ::Payload::Pipeline getPayload();
private:
  ::capnp::AnyPointer::Pipeline _typeless;
  friend class ::capnp::PipelineHook;
  template <typename, ::capnp::Kind>
  friend struct ::capnp::ToDynamic_;
};
#endif  // !CAPNP_LITE

// =======================================================================================

inline  ::uint8_t Payload::Reader::getBits() const {
  return _reader.getDataField< ::uint8_t>(
      ::capnp::bounded<0>() * ::capnp::ELEMENTS);
}

inline  ::uint8_t Payload::Builder::getBits() {
  return _builder.getDataField< ::uint8_t>(
      ::capnp::bounded<0>() * ::capnp::ELEMENTS);
}
inline void Payload::Builder::setBits( ::uint8_t value) {
  _builder.setDataField< ::uint8_t>(
      ::capnp::bounded<0>() * ::capnp::ELEMENTS, value);
}

inline bool Payload::Reader::hasBytes() const {
  return !_reader.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS).isNull();
}
inline bool Payload::Builder::hasBytes() {
  return !_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS).isNull();
}
inline  ::capnp::Data::Reader Payload::Reader::getBytes() const {
  return ::capnp::_::PointerHelpers< ::capnp::Data>::get(_reader.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS));
}
inline  ::capnp::Data::Builder Payload::Builder::getBytes() {
  return ::capnp::_::PointerHelpers< ::capnp::Data>::get(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS));
}
inline void Payload::Builder::setBytes( ::capnp::Data::Reader value) {
  ::capnp::_::PointerHelpers< ::capnp::Data>::set(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS), value);
}
inline  ::capnp::Data::Builder Payload::Builder::initBytes(unsigned int size) {
  return ::capnp::_::PointerHelpers< ::capnp::Data>::init(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS), size);
}
inline void Payload::Builder::adoptBytes(
    ::capnp::Orphan< ::capnp::Data>&& value) {
  ::capnp::_::PointerHelpers< ::capnp::Data>::adopt(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS), kj::mv(value));
}
inline ::capnp::Orphan< ::capnp::Data> Payload::Builder::disownBytes() {
  return ::capnp::_::PointerHelpers< ::capnp::Data>::disown(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS));
}

inline  ::uint64_t Child::Reader::getOffset() const {
  return _reader.getDataField< ::uint64_t>(
      ::capnp::bounded<0>() * ::capnp::ELEMENTS);
}

inline  ::uint64_t Child::Builder::getOffset() {
  return _builder.getDataField< ::uint64_t>(
      ::capnp::bounded<0>() * ::capnp::ELEMENTS);
}
inline void Child::Builder::setOffset( ::uint64_t value) {
  _builder.setDataField< ::uint64_t>(
      ::capnp::bounded<0>() * ::capnp::ELEMENTS, value);
}

inline  ::uint8_t Child::Reader::getTransition() const {
  return _reader.getDataField< ::uint8_t>(
      ::capnp::bounded<8>() * ::capnp::ELEMENTS);
}

inline  ::uint8_t Child::Builder::getTransition() {
  return _builder.getDataField< ::uint8_t>(
      ::capnp::bounded<8>() * ::capnp::ELEMENTS);
}
inline void Child::Builder::setTransition( ::uint8_t value) {
  _builder.setDataField< ::uint8_t>(
      ::capnp::bounded<8>() * ::capnp::ELEMENTS, value);
}

inline bool Node::Reader::hasPayload() const {
  return !_reader.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS).isNull();
}
inline bool Node::Builder::hasPayload() {
  return !_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS).isNull();
}
inline  ::Payload::Reader Node::Reader::getPayload() const {
  return ::capnp::_::PointerHelpers< ::Payload>::get(_reader.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS));
}
inline  ::Payload::Builder Node::Builder::getPayload() {
  return ::capnp::_::PointerHelpers< ::Payload>::get(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS));
}
#if !CAPNP_LITE
inline  ::Payload::Pipeline Node::Pipeline::getPayload() {
  return  ::Payload::Pipeline(_typeless.getPointerField(0));
}
#endif  // !CAPNP_LITE
inline void Node::Builder::setPayload( ::Payload::Reader value) {
  ::capnp::_::PointerHelpers< ::Payload>::set(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS), value);
}
inline  ::Payload::Builder Node::Builder::initPayload() {
  return ::capnp::_::PointerHelpers< ::Payload>::init(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS));
}
inline void Node::Builder::adoptPayload(
    ::capnp::Orphan< ::Payload>&& value) {
  ::capnp::_::PointerHelpers< ::Payload>::adopt(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS), kj::mv(value));
}
inline ::capnp::Orphan< ::Payload> Node::Builder::disownPayload() {
  return ::capnp::_::PointerHelpers< ::Payload>::disown(_builder.getPointerField(
      ::capnp::bounded<0>() * ::capnp::POINTERS));
}

inline bool Node::Reader::hasChildren() const {
  return !_reader.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS).isNull();
}
inline bool Node::Builder::hasChildren() {
  return !_builder.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS).isNull();
}
inline  ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Reader Node::Reader::getChildren() const {
  return ::capnp::_::PointerHelpers< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>::get(_reader.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS));
}
inline  ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Builder Node::Builder::getChildren() {
  return ::capnp::_::PointerHelpers< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>::get(_builder.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS));
}
inline void Node::Builder::setChildren( ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Reader value) {
  ::capnp::_::PointerHelpers< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>::set(_builder.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS), value);
}
inline  ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>::Builder Node::Builder::initChildren(unsigned int size) {
  return ::capnp::_::PointerHelpers< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>::init(_builder.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS), size);
}
inline void Node::Builder::adoptChildren(
    ::capnp::Orphan< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>&& value) {
  ::capnp::_::PointerHelpers< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>::adopt(_builder.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS), kj::mv(value));
}
inline ::capnp::Orphan< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>> Node::Builder::disownChildren() {
  return ::capnp::_::PointerHelpers< ::capnp::List< ::Child,  ::capnp::Kind::STRUCT>>::disown(_builder.getPointerField(
      ::capnp::bounded<1>() * ::capnp::POINTERS));
}


CAPNP_END_HEADER
