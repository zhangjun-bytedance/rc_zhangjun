package api

import (
	"crypto/rand"
	"encoding/binary"
	"strings"
	"time"
)

// crockfordBase32 是 Crockford Base32 字母表：去掉了 I、L、O、U，
// 避免人眼在工单和日志里把 0/O、1/I/l 看错——通知 ID 是排障时要被人复制粘贴的东西。
const crockfordBase32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newNotificationID 生成一个 26 字符的时间有序 ID（ULID 布局：48 位毫秒 + 80 位随机）。
//
// 为什么不用 UUIDv4：ID 是主键，随机主键会让每次插入落在 B-tree 的随机位置，
// 造成页分裂和缓存失效。时间有序 ID 让插入始终追加在索引末尾，
// 同时让「按 ID 排序」天然等于「按创建时间排序」，排障时很实用。
//
// 为什么不用自增 ID：多实例写同一张表时自增需要中心化协调，
// 而且自增 ID 会泄露业务量级。
func newNotificationID() string {
	var buf [16]byte
	ms := uint64(time.Now().UTC().UnixMilli())
	// 前 6 字节存毫秒时间戳（大端，保证字典序 == 时间序）。
	binary.BigEndian.PutUint64(buf[0:8], ms<<16)
	// 后 10 字节填随机数。
	if _, err := rand.Read(buf[6:]); err != nil {
		// crypto/rand 失败意味着系统熵源出了问题，这种情况下继续生成
		// 可能碰撞的 ID 比直接崩掉更危险（幂等和去重都建立在 ID 唯一之上）。
		panic("api: crypto/rand unavailable: " + err.Error())
	}
	return encodeBase32(buf)
}

// encodeBase32 把 16 字节编码成 26 个 Crockford Base32 字符。
func encodeBase32(b [16]byte) string {
	var sb strings.Builder
	sb.Grow(26)
	// 26 个字符 * 5 bit = 130 bit，比 128 bit 多 2 bit，所以首字符只取高 3 位。
	var bitBuf uint16
	var bitCount uint
	// 先补 2 个 0 bit 让总位数对齐到 130。
	bitBuf = 0
	bitCount = 2
	for _, c := range b {
		bitBuf = bitBuf<<8 | uint16(c)
		bitCount += 8
		for bitCount >= 5 {
			bitCount -= 5
			sb.WriteByte(crockfordBase32[(bitBuf>>bitCount)&0x1f])
		}
	}
	return sb.String()
}
