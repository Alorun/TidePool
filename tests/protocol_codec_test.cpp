#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tidepool/api/block.h"
#include "tidepool/api/block_codec.h"
#include "tidepool/protocol/wire_codec.h"

using namespace tidepool;
using namespace tidepool::wire;

namespace {

int g_checks = 0;

#define CHECK(cond, msg)                                                                   \
    do {                                                                                   \
        ++g_checks;                                                                        \
        if (!(cond)) {                                                                     \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

const std::uint8_t* DataOrNull(const ByteBuffer& bytes) {
    return bytes.empty() ? nullptr : bytes.data();
}

std::string BytesToString(const ByteBuffer& bytes) {
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

Block MakeBlock() {
    Block block;
    block.metadata.num_tokens = 0x01020304U;
    block.metadata.num_layers = 0x11121314U;
    block.metadata.dtype_size = 0x2122U;
    block.metadata.kv_heads = 0x3132U;
    block.metadata.created_unix_ns = 0x4142434445464748ULL;
    block.metadata.model_fingerprint = 0x51525354U;
    block.data = {0x00, 0x7f, 0x80, 0xff, 0x42};
    return block;
}

BlockKey GoldenKey() {
    return BlockKey{0x0102030405060708ULL, 0x11121314U, 0x21222324U};
}

FrameHeader Request(Opcode opcode, std::uint64_t request_id,
                    std::uint64_t payload_bytes) {
    return FrameHeader{MessageKind::kRequest, opcode, WireStatus::kOk, 0,
                       request_id, payload_bytes};
}

FrameHeader Response(Opcode opcode, WireStatus status,
                     std::uint64_t request_id,
                     std::uint64_t payload_bytes) {
    return FrameHeader{MessageKind::kResponse, opcode, status, 0, request_id,
                       payload_bytes};
}

void PutU16ForTest(EncodedHeader* bytes, std::size_t offset,
                   std::uint16_t value) {
    (*bytes)[offset] = static_cast<std::uint8_t>(value >> 8U);
    (*bytes)[offset + 1] = static_cast<std::uint8_t>(value & 0xffU);
}

void PutU64ForTest(EncodedHeader* bytes, std::size_t offset,
                   std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        (*bytes)[offset + i] = static_cast<std::uint8_t>(
            value >> static_cast<unsigned>((7U - i) * 8U));
    }
}

void CheckHeaderEqual(const FrameHeader& actual, const FrameHeader& expected,
                      const char* message) {
    CHECK(actual.kind == expected.kind && actual.opcode == expected.opcode &&
              actual.status == expected.status && actual.flags == expected.flags &&
              actual.request_id == expected.request_id &&
              actual.payload_bytes == expected.payload_bytes,
          message);
}

void TestConstantsAndStatusMapping() {
    CHECK(kMagic == 0x54504e57U, "wire magic is frozen");
    CHECK(kVersion == 1 && kHeaderBytes == 32 && kBlockKeyBytes == 16,
          "wire sizes are frozen");
    CHECK(kDefaultMaxFrameBytes == 256ULL * 1024ULL * 1024ULL,
          "default frame limit is 256 MiB");
    CHECK(static_cast<std::uint8_t>(MessageKind::kRequest) == 1 &&
              static_cast<std::uint8_t>(MessageKind::kResponse) == 2,
          "message kind values are frozen");
    CHECK(static_cast<std::uint8_t>(Opcode::kGet) == 1 &&
              static_cast<std::uint8_t>(Opcode::kPut) == 2 &&
              static_cast<std::uint8_t>(Opcode::kPing) == 3,
          "opcode values are frozen");

    struct Mapping {
        StatusCode local;
        WireStatus wire;
        std::uint16_t number;
    };
    const std::array<Mapping, 11> mappings{{
        {StatusCode::kOk, WireStatus::kOk, 0},
        {StatusCode::kNotImplemented, WireStatus::kNotImplemented, 1},
        {StatusCode::kNotFound, WireStatus::kNotFound, 2},
        {StatusCode::kInvalidArgument, WireStatus::kInvalidArgument, 3},
        {StatusCode::kAlreadyExists, WireStatus::kAlreadyExists, 4},
        {StatusCode::kIoError, WireStatus::kIoError, 5},
        {StatusCode::kNetworkError, WireStatus::kNetworkError, 6},
        {StatusCode::kUnavailable, WireStatus::kUnavailable, 7},
        {StatusCode::kOutOfCapacity, WireStatus::kOutOfCapacity, 8},
        {StatusCode::kCorruption, WireStatus::kCorruption, 9},
        {StatusCode::kInternal, WireStatus::kInternal, 10},
    }};
    for (const Mapping& mapping : mappings) {
        auto encoded = ToWireStatus(mapping.local);
        CHECK(encoded.ok() && encoded.value() == mapping.wire,
              "StatusCode maps explicitly to WireStatus");
        CHECK(static_cast<std::uint16_t>(mapping.wire) == mapping.number,
              "WireStatus numeric value is fixed");
        auto decoded = FromWireStatus(mapping.wire);
        CHECK(decoded.ok() && decoded.value() == mapping.local,
              "WireStatus maps explicitly to StatusCode");
    }
    CHECK(!ToWireStatus(static_cast<StatusCode>(999)).ok(),
          "unknown local status is not mapped");
    auto unknown_wire = FromWireStatus(static_cast<WireStatus>(999));
    CHECK(!unknown_wire.ok() &&
              unknown_wire.status().code() == StatusCode::kCorruption,
          "unknown wire status is corruption");
}

void TestHeaderGoldenVector() {
    const FrameHeader header =
        Response(Opcode::kGet, WireStatus::kNotFound,
                 0x0102030405060708ULL, 7);
    auto encoded = EncodeHeader(header);
    CHECK(encoded.ok(), "golden header encodes");

    const EncodedHeader expected{{
        0x54, 0x50, 0x4e, 0x57,  // magic TPNW
        0x00, 0x01,              // version
        0x00, 0x20,              // header bytes
        0x02,                    // response
        0x01,                    // Get
        0x00, 0x02,              // NotFound
        0x00, 0x00, 0x00, 0x00,  // flags
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,  // request id
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,  // payload
    }};
    CHECK(encoded.value() == expected,
          "header encoder matches hand-written big-endian bytes");

    auto decoded = DecodeHeader(expected.data(), expected.size());
    CHECK(decoded.ok(), "golden header decodes");
    CheckHeaderEqual(decoded.value(), header, "golden header fields match");
}

void TestHeaderRoundTrips() {
    const std::array<FrameHeader, 8> headers{{
        Request(Opcode::kGet, 0, 16),
        Request(Opcode::kPut, std::numeric_limits<std::uint64_t>::max(), 17),
        Request(Opcode::kPing, 0x1020304050607080ULL, 0),
        Response(Opcode::kGet, WireStatus::kOk, 1, 36),
        Response(Opcode::kPut, WireStatus::kOk, 2, 0),
        Response(Opcode::kPing, WireStatus::kOk, 3, 0),
        Response(Opcode::kGet, WireStatus::kInternal, 4, 4),
        Response(Opcode::kPing, WireStatus::kUnavailable, 5, 9),
    }};
    for (const FrameHeader& header : headers) {
        auto encoded = EncodeHeader(header);
        CHECK(encoded.ok(), "valid header variant encodes");
        auto decoded = DecodeHeader(encoded.value().data(), encoded.value().size());
        CHECK(decoded.ok(), "valid header variant decodes");
        CheckHeaderEqual(decoded.value(), header, "header round-trip is lossless");
    }
}

void TestFrameSizeBoundaries() {
    CodecLimits limits;
    auto zero = ValidateFrameSize(0, limits);
    CHECK(zero.ok() && zero.value() == kHeaderBytes,
          "zero payload frame is header-sized");

    const std::uint64_t boundary = limits.max_frame_bytes - kHeaderBytes;
    auto exact = ValidateFrameSize(boundary, limits);
    CHECK(exact.ok() && exact.value() == limits.max_frame_bytes,
          "payload exactly at max frame boundary succeeds");
    CHECK(!ValidateFrameSize(boundary + 1, limits).ok(),
          "payload one byte above frame boundary fails");

    CodecLimits too_small;
    too_small.max_frame_bytes = kHeaderBytes - 1;
    CHECK(!ValidateFrameSize(0, too_small).ok(),
          "max frame smaller than header fails before subtraction");

    CodecLimits unlimited;
    unlimited.max_frame_bytes = std::numeric_limits<std::uint64_t>::max();
    CHECK(!ValidateFrameSize(std::numeric_limits<std::uint64_t>::max(),
                             unlimited)
               .ok(),
          "uint64 max payload cannot overflow header addition");

    CodecLimits small;
    small.max_frame_bytes = 64;
    CHECK(ValidateFrameSize(32, small).ok(),
          "small equivalent frame boundary succeeds");
    CHECK(!ValidateFrameSize(33, small).ok(),
          "small equivalent boundary plus one fails");
}

void TestInvalidHeaders() {
    const FrameHeader baseline = Request(Opcode::kGet, 9, 16);
    auto encoded_result = EncodeHeader(baseline);
    CHECK(encoded_result.ok(), "invalid-header baseline encodes");
    const EncodedHeader encoded = encoded_result.value();

    CHECK(!DecodeHeader(encoded.data(), kHeaderBytes - 1).ok(),
          "31-byte header is truncated");
    std::array<std::uint8_t, kHeaderBytes + 1> extra{};
    std::copy(encoded.begin(), encoded.end(), extra.begin());
    CHECK(!DecodeHeader(extra.data(), extra.size()).ok(),
          "33-byte fixed header input has trailing data");
    CHECK(!DecodeHeader(nullptr, kHeaderBytes).ok(),
          "null header pointer is rejected");

    auto bad = encoded;
    bad[0] ^= 0xffU;
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(), "bad magic fails");
    bad = encoded;
    PutU16ForTest(&bad, 4, 2);
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(),
          "unsupported version fails");
    bad = encoded;
    PutU16ForTest(&bad, 6, 31);
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(),
          "wrong header size fails");
    bad = encoded;
    bad[8] = 0xffU;
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(), "unknown kind fails");
    bad = encoded;
    bad[9] = 0xffU;
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(), "unknown opcode fails");
    bad = encoded;
    PutU16ForTest(&bad, 10, 0xffffU);
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(), "unknown status fails");
    bad = encoded;
    bad[15] = 1;
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(), "nonzero flags fail");
    bad = encoded;
    PutU16ForTest(&bad, 10,
                  static_cast<std::uint16_t>(WireStatus::kNotFound));
    CHECK(!DecodeHeader(bad.data(), bad.size()).ok(),
          "request with non-OK status fails");

    CodecLimits short_limit;
    short_limit.max_frame_bytes = 31;
    CHECK(!DecodeHeader(encoded.data(), encoded.size(), short_limit).ok(),
          "decode rejects max frame below header size");

    CodecLimits boundary_limit;
    boundary_limit.max_frame_bytes = 64;
    auto boundary_header = EncodeHeader(Request(Opcode::kPut, 1, 32),
                                        boundary_limit);
    CHECK(boundary_header.ok(), "header at custom frame boundary encodes");
    CHECK(DecodeHeader(boundary_header.value().data(), kHeaderBytes,
                       boundary_limit)
              .ok(),
          "header at custom frame boundary decodes");
    bad = boundary_header.value();
    PutU64ForTest(&bad, 24, 33);
    CHECK(!DecodeHeader(bad.data(), bad.size(), boundary_limit).ok(),
          "declared payload over limit by one fails");

    CodecLimits huge_limit;
    huge_limit.max_frame_bytes = std::numeric_limits<std::uint64_t>::max();
    bad = encoded;
    PutU64ForTest(&bad, 24, std::numeric_limits<std::uint64_t>::max());
    CHECK(!DecodeHeader(bad.data(), bad.size(), huge_limit).ok(),
          "uint64 max declared payload fails without allocation");

    CHECK(!EncodeHeader(Request(Opcode::kGet, 1, 15)).ok(),
          "Get request length 15 is invalid at encode");
    CHECK(!EncodeHeader(Request(Opcode::kGet, 1, 17)).ok(),
          "Get request length 17 is invalid at encode");
    CHECK(!EncodeHeader(Request(Opcode::kPut, 1, 16)).ok(),
          "Put request without Block blob is invalid at encode");
    CHECK(!EncodeHeader(Request(Opcode::kPing, 1, 1)).ok(),
          "Ping request with payload is invalid at encode");
    CHECK(!EncodeHeader(Response(Opcode::kGet, WireStatus::kOk, 1, 0)).ok(),
          "successful Get with empty payload is invalid at encode");
    CHECK(!EncodeHeader(Response(Opcode::kPut, WireStatus::kOk, 1, 1)).ok(),
          "successful Put with payload is invalid at encode");
    CHECK(!EncodeHeader(Response(Opcode::kPing, WireStatus::kOk, 1, 1)).ok(),
          "successful Ping with payload is invalid at encode");
    CHECK(!EncodeHeader(Response(Opcode::kGet, WireStatus::kNotFound, 1, 3)).ok(),
          "error response shorter than length prefix is invalid");
}

void TestBlockKeyCodec() {
    const BlockKey key = GoldenKey();
    auto encoded = EncodeBlockKey(key);
    CHECK(encoded.ok(), "BlockKey encodes");
    const EncodedBlockKey expected{{
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x11, 0x12, 0x13, 0x14,
        0x21, 0x22, 0x23, 0x24,
    }};
    CHECK(encoded.value() == expected,
          "BlockKey matches hand-written big-endian golden vector");
    auto decoded = DecodeBlockKey(expected.data(), expected.size());
    CHECK(decoded.ok() && decoded.value() == key,
          "golden BlockKey round-trips");
    CHECK(!DecodeBlockKey(expected.data(), expected.size() - 1).ok(),
          "15-byte BlockKey fails");
    std::array<std::uint8_t, kBlockKeyBytes + 1> extra{};
    std::copy(expected.begin(), expected.end(), extra.begin());
    CHECK(!DecodeBlockKey(extra.data(), extra.size()).ok(),
          "17-byte BlockKey fails");
    CHECK(!DecodeBlockKey(nullptr, kBlockKeyBytes).ok(),
          "null BlockKey pointer fails");

    const BlockKey max_key{std::numeric_limits<std::uint64_t>::max(),
                           std::numeric_limits<std::uint32_t>::max(),
                           std::numeric_limits<std::uint32_t>::max()};
    auto max_encoded = EncodeBlockKey(max_key);
    auto max_decoded = DecodeBlockKey(max_encoded.value().data(),
                                      max_encoded.value().size());
    CHECK(max_decoded.ok() && max_decoded.value() == max_key,
          "maximum BlockKey fields round-trip losslessly");
}

void TestGetPayloads() {
    const BlockKey key = GoldenKey();
    auto request = EncodeGetRequest(key);
    CHECK(request.ok() && request.value().size() == kBlockKeyBytes,
          "Get request is exactly one BlockKey");
    auto decoded_key = DecodeGetRequest(request.value().data(),
                                        request.value().size());
    CHECK(decoded_key.ok() && decoded_key.value() == key,
          "Get request decodes key");
    CHECK(!DecodeGetRequest(request.value().data(), 15).ok(),
          "short Get request fails");
    ByteBuffer extra = request.value();
    extra.push_back(0);
    CHECK(!DecodeGetRequest(extra.data(), extra.size()).ok(),
          "Get request with trailing byte fails");

    const std::string blob = SerializeBlock(MakeBlock());
    auto success = EncodeGetSuccessPayload(blob);
    CHECK(success.ok() && BytesToString(success.value()) == blob,
          "Get success encoder preserves Block blob bytes");
    auto decoded_blob = DecodeGetSuccessPayload(success.value().data(),
                                                success.value().size());
    CHECK(decoded_blob.ok() && decoded_blob.value() == blob,
          "Get success decoder preserves Block blob bytes");
    success.value()[0] ^= 0xffU;
    CHECK(decoded_blob.value() == blob,
          "decoded Get Block blob owns its bytes");
    CHECK(!EncodeGetSuccessPayload("").ok(),
          "empty successful Get payload fails encoding");
    CHECK(!DecodeGetSuccessPayload(nullptr, 0).ok(),
          "empty successful Get payload fails decoding");

    FrameHeader request_header = Request(Opcode::kGet, 77, request.value().size());
    CHECK(ValidateRequest(request_header, request.value().data(),
                          request.value().size())
              .ok(),
          "complete Get request validates");
    request_header.payload_bytes--;
    CHECK(!ValidateRequest(request_header, request.value().data(),
                           request.value().size())
               .ok(),
          "Get request declared length mismatch fails");

    FrameHeader response_header =
        Response(Opcode::kGet, WireStatus::kOk, 77, success.value().size());
    CHECK(ValidateResponse(response_header, success.value().data(),
                           success.value().size())
              .ok(),
          "complete Get success response validates");

    auto error = EncodeErrorPayload("missing");
    FrameHeader error_header = Response(Opcode::kGet, WireStatus::kNotFound,
                                        77, error.value().size());
    CHECK(ValidateResponse(error_header, error.value().data(),
                           error.value().size())
              .ok(),
          "Get error response validates as ErrorPayload");

    // There is no outer payload tag beyond status. Under kOk, any non-empty
    // byte string is the opaque Block blob; only the existing block_codec can
    // distinguish a real SerializeBlock blob from ErrorPayload-shaped bytes.
    FrameHeader mislabeled_success =
        Response(Opcode::kGet, WireStatus::kOk, 77, error.value().size());
    CHECK(ValidateResponse(mislabeled_success, error.value().data(),
                           error.value().size())
              .ok(),
          "wire layer keeps successful Get Block blob opaque");
    auto opaque = DecodeGetSuccessPayload(error.value().data(),
                                          error.value().size());
    CHECK(opaque.ok() && !DeserializeHeader(opaque.value(), nullptr, nullptr,
                                            nullptr)
                              .ok(),
          "RPC block_codec layer rejects ErrorPayload bytes mislabeled as a Block blob");

    error_header.payload_bytes = success.value().size();
    CHECK(!ValidateResponse(error_header, success.value().data(),
                            success.value().size())
               .ok(),
          "error status with Block blob fails ErrorPayload validation");
}

void TestPutPayloads() {
    const BlockKey key = GoldenKey();
    const std::string blob = SerializeBlock(MakeBlock());
    auto request = EncodePutRequest(key, blob);
    CHECK(request.ok() && request.value().size() == kBlockKeyBytes + blob.size(),
          "Put request contains key followed by Block blob");
    auto decoded = DecodePutRequest(request.value().data(), request.value().size());
    CHECK(decoded.ok() && decoded.value().key == key &&
              decoded.value().block_blob == blob,
          "Put request round-trips key and owned Block blob");
    request.value()[kBlockKeyBytes] ^= 0xffU;
    CHECK(decoded.value().block_blob == blob,
          "decoded Put request owns its Block blob");
    CHECK(!DecodePutRequest(request.value().data(), 15).ok(),
          "Put request shorter than BlockKey fails");
    CHECK(!DecodePutRequest(request.value().data(), 16).ok(),
          "Put request with only BlockKey fails");
    CHECK(!EncodePutRequest(key, "").ok(),
          "Put request with empty Block blob fails encoding");

    CodecLimits tight;
    tight.max_frame_bytes = kHeaderBytes + kBlockKeyBytes + blob.size() - 1;
    CHECK(!EncodePutRequest(key, blob, tight).ok(),
          "Put encoder validates total payload against frame limit");

    FrameHeader request_header =
        Request(Opcode::kPut, 88, request.value().size());
    CHECK(ValidateRequest(request_header, request.value().data(),
                          request.value().size())
              .ok(),
          "complete Put request validates");
    CHECK(!ValidateRequest(request_header, nullptr, request.value().size()).ok(),
          "nonempty Put request with null pointer fails");

    auto empty = EncodeEmptySuccessPayload();
    CHECK(empty.ok() && empty.value().empty(),
          "Put success payload encoder is empty");
    FrameHeader success_header = Response(Opcode::kPut, WireStatus::kOk, 88, 0);
    CHECK(ValidateResponse(success_header, nullptr, 0).ok(),
          "empty Put success response validates");
    const std::uint8_t extra = 0;
    success_header.payload_bytes = 1;
    CHECK(!ValidateResponse(success_header, &extra, 1).ok(),
          "Put success response with extra byte fails");

    auto error = EncodeErrorPayload("write failed");
    FrameHeader error_header = Response(Opcode::kPut, WireStatus::kIoError, 88,
                                        error.value().size());
    CHECK(ValidateResponse(error_header, error.value().data(),
                           error.value().size())
              .ok(),
          "Put error response validates");
}

void TestPingPayloads() {
    auto request = EncodePingRequest();
    CHECK(request.ok() && request.value().empty(),
          "Ping request encoder is empty");
    CHECK(DecodePingRequest(nullptr, 0).ok(), "empty Ping request decodes");
    const std::uint8_t byte = 1;
    CHECK(!DecodePingRequest(&byte, 1).ok(),
          "Ping request with extra byte fails");

    FrameHeader request_header = Request(Opcode::kPing, 1, 0);
    CHECK(ValidateRequest(request_header, nullptr, 0).ok(),
          "Ping request with nonzero request_id validates");
    FrameHeader success_header = Response(Opcode::kPing, WireStatus::kOk, 1, 0);
    CHECK(ValidateResponse(success_header, nullptr, 0).ok(),
          "Ping success response validates");
    success_header.payload_bytes = 1;
    CHECK(!ValidateResponse(success_header, &byte, 1).ok(),
          "Ping success response with payload fails");

    auto error = EncodeErrorPayload("unavailable");
    FrameHeader error_header = Response(Opcode::kPing, WireStatus::kUnavailable,
                                        1, error.value().size());
    CHECK(ValidateResponse(error_header, error.value().data(),
                           error.value().size())
              .ok(),
          "Ping error response validates");
}

void TestErrorPayloads() {
    const std::array<std::string, 4> messages{{
        "ordinary ASCII",
        u8"中文错误",
        "",
        std::string("left\0right", 10),
    }};
    for (const std::string& message : messages) {
        auto encoded = EncodeErrorPayload(message);
        CHECK(encoded.ok() && encoded.value().size() == 4 + message.size(),
              "error payload has exact prefixed length");
        auto decoded = DecodeErrorPayload(DataOrNull(encoded.value()),
                                          encoded.value().size());
        CHECK(decoded.ok() && decoded.value() == message,
              "error payload round-trips arbitrary UTF-8 bytes");
    }

    const std::array<std::uint8_t, 3> short_prefix{{0, 0, 0}};
    CHECK(!DecodeErrorPayload(short_prefix.data(), short_prefix.size()).ok(),
          "truncated error length field fails");
    const std::array<std::uint8_t, 5> declares_two{{0, 0, 0, 2, 'x'}};
    CHECK(!DecodeErrorPayload(declares_two.data(), declares_two.size()).ok(),
          "declared error length larger than payload fails");
    const std::array<std::uint8_t, 6> declares_one{{0, 0, 0, 1, 'x', 'y'}};
    CHECK(!DecodeErrorPayload(declares_one.data(), declares_one.size()).ok(),
          "trailing error payload byte fails");
    const std::array<std::uint8_t, 4> declares_max{{0xff, 0xff, 0xff, 0xff}};
    CHECK(!DecodeErrorPayload(declares_max.data(), declares_max.size()).ok(),
          "UINT32_MAX error declaration fails before allocation");
    CHECK(!DecodeErrorPayload(nullptr, 4).ok(),
          "null error payload pointer fails");

    CodecLimits small;
    small.max_error_message_bytes = 3;
    auto at_limit = EncodeErrorPayload("abc", small);
    CHECK(at_limit.ok(), "error message exactly at configured limit succeeds");
    CHECK(!EncodeErrorPayload("abcd", small).ok(),
          "error message over configured limit fails");
    CHECK(DecodeErrorPayload(at_limit.value().data(), at_limit.value().size(),
                             small)
              .ok(),
          "error decoder accepts configured limit boundary");

    CodecLimits frame_tight;
    frame_tight.max_frame_bytes = kHeaderBytes + 4;
    CHECK(EncodeErrorPayload("", frame_tight).ok(),
          "empty error message fits four-byte payload");
    CHECK(!EncodeErrorPayload("x", frame_tight).ok(),
          "error encoder applies outer frame limit");

    const std::string default_limit(kDefaultMaxErrorMessageBytes, 'x');
    CHECK(EncodeErrorPayload(default_limit).ok(),
          "error message exactly at default 4 KiB limit succeeds");
    CHECK(!EncodeErrorPayload(default_limit + "x").ok(),
          "error message above default 4 KiB limit fails");
}

void TestSemanticValidation() {
    const BlockKey key = GoldenKey();
    auto get = EncodeGetRequest(key);
    FrameHeader get_header = Request(Opcode::kGet, 1, get.value().size());

    FrameHeader wrong_kind = get_header;
    wrong_kind.kind = MessageKind::kResponse;
    CHECK(!ValidateRequest(wrong_kind, get.value().data(), get.value().size()).ok(),
          "request validator rejects response kind");
    CHECK(!ValidateResponse(get_header, get.value().data(), get.value().size()).ok(),
          "response validator rejects request kind");

    FrameHeader zero_request_id = Request(Opcode::kGet, 0, get.value().size());
    CHECK(!ValidateRequest(zero_request_id, get.value().data(), get.value().size()).ok(),
          "request semantic validation rejects reserved request_id zero");
    FrameHeader zero_response_id =
        Response(Opcode::kGet, WireStatus::kOk, 0, get.value().size());
    CHECK(!ValidateResponse(zero_response_id, get.value().data(), get.value().size()).ok(),
          "response semantic validation rejects reserved request_id zero");

    FrameHeader unknown = get_header;
    unknown.opcode = static_cast<Opcode>(0xff);
    CHECK(!ValidateRequest(unknown, get.value().data(), get.value().size()).ok(),
          "complete request validator rejects unknown opcode");
    unknown = get_header;
    unknown.status = static_cast<WireStatus>(0xffff);
    CHECK(!ValidateRequest(unknown, get.value().data(), get.value().size()).ok(),
          "complete request validator rejects unknown status");
    unknown = get_header;
    unknown.flags = 1;
    CHECK(!ValidateRequest(unknown, get.value().data(), get.value().size()).ok(),
          "complete request validator rejects nonzero flags");

    // A Block blob is intentionally opaque at the wire layer. A non-empty
    // byte is structurally valid here; the RPC integration must invoke the
    // existing block_codec before constructing a Block.
    ByteBuffer opaque_put(kBlockKeyBytes + 1, 0);
    auto key_bytes = EncodeBlockKey(key);
    std::copy(key_bytes.value().begin(), key_bytes.value().end(),
              opaque_put.begin());
    auto decoded_opaque = DecodePutRequest(opaque_put.data(), opaque_put.size());
    CHECK(decoded_opaque.ok() && decoded_opaque.value().block_blob.size() == 1,
          "wire codec leaves non-empty Put Block blob opaque");
}

void TestBlockBlobInvariance() {
    const Block block = MakeBlock();
    const BlockKey key = GoldenKey();
    const std::string original = SerializeBlock(block);

    auto get_encoded = EncodeGetSuccessPayload(original);
    auto get_decoded = DecodeGetSuccessPayload(get_encoded.value().data(),
                                               get_encoded.value().size());
    CHECK(get_decoded.ok() && get_decoded.value() == original,
          "SerializeBlock -> Get payload -> decode is byte-identical");

    auto put_encoded = EncodePutRequest(key, original);
    auto put_decoded = DecodePutRequest(put_encoded.value().data(),
                                        put_encoded.value().size());
    CHECK(put_decoded.ok() && put_decoded.value().block_blob == original,
          "SerializeBlock -> Put payload -> decode is byte-identical");

    BlockMetadata metadata;
    std::size_t payload_len = 0;
    std::size_t payload_offset = 0;
    CHECK(DeserializeHeader(put_decoded.value().block_blob, &metadata,
                            &payload_len, &payload_offset)
              .ok(),
          "unchanged network Block blob remains valid to block_codec");
    CHECK(payload_offset == block_codec::kHeaderSize &&
              payload_len == block.data.size() &&
              metadata.created_unix_ns == block.metadata.created_unix_ns,
          "block_codec metadata and payload length remain unchanged");
}

}  // namespace

int main() {
    TestConstantsAndStatusMapping();
    TestHeaderGoldenVector();
    TestHeaderRoundTrips();
    TestFrameSizeBoundaries();
    TestInvalidHeaders();
    TestBlockKeyCodec();
    TestGetPayloads();
    TestPutPayloads();
    TestPingPayloads();
    TestErrorPayloads();
    TestSemanticValidation();
    TestBlockBlobInvariance();

    std::printf("tidepool protocol codec test: %d checks passed\n", g_checks);
    return 0;
}
