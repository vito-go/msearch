// Package msearch  基于mmap技术的，以本地文件为基础的搜索技术。提供增加、删、查（简单的替代mysql。）
// 单个 value 长度不能超过255. // todo if needed?
// [_8(total) _1 key  _1(len) xxx _1(len) xxx  _8(next) _8(overflow offset)]

package msearch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

// notExist 标记不存在的key.
const notExist = -1

// DefaultLength 默认映射空间大小 32GB，不影响实际内存大小。
const DefaultLength = 32 << 30

type Msearch struct {
	mu        sync.RWMutex // to protect the follow fields
	f         *os.File
	offset    int
	keyMap    map[string]int
	bytesAddr []byte
}

func NewMsearch(file string, length int) (*Msearch, error) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if length <= 0 {
		length = DefaultLength
	}
	// 追加用f.Write 读取和修改用MMap
	bytesAddr, err := syscall.Mmap(int(f.Fd()), 0, length, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &Msearch{
		mu:        sync.RWMutex{},
		f:         f,
		offset:    0,
		keyMap:    make(map[string]int, 1<<10),
		bytesAddr: bytesAddr,
	}, nil
}

// Add 增.
func (s *Msearch) Add(key string, values ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adds(key, values...)
}

func (s *Msearch) adds(key string, values ...string) error {
	if len(values) == 0 {
		return nil
	}
	offset, ok := s.keyMap[key]
	// 不存在
	if !ok || offset == notExist {
		_, err := s.add(nil, key, values...)
		return err
	}
	// t 是否能插空 插空进入
	// s.bytesAddr[offset:offset+8]
	if len(values) == 1 {
		value := values[0]
		o, start, end, t := s.empty(offset)
		if t && len(value) < (end-start) {
			total := bigUint64(s.bytesAddr[offset : offset+8])
			b := s.bytesAddr[o : o+total]
			b[start] = byte(len(value))
			copy(b[start+1:], value)
			return nil
		}
	}
	b8 := s.getB8byOffset(offset)
	_, err := s.add(b8, key, values...)
	return err
}

// Del 删.
func (s *Msearch) Del(key string, values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dels(key, values...)
}

func (s *Msearch) dels(key string, values ...string) {
	offset, ok := s.keyMap[key]
	if !ok {
		return
	}
	valueMap := make(map[string]struct{}, len(values))
	for _, value := range values {
		valueMap[value] = struct{}{}
	}
	if len(valueMap) == 0 {
		return
	}
	for {
		d := s.del(offset, valueMap)
		if d == 0 {
			break
		}
		offset = d
	}
}

// Get 查
func (s *Msearch) Get(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gets(key)
}

func (s *Msearch) Update(key string, values ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldValues := s.gets(key)
	s.dels(key, oldValues...)
	err := s.adds(key, values...)
	return err
}

func (s *Msearch) gets(key string) []string {
	offset, ok := s.keyMap[key]
	if !ok || offset == notExist {
		return nil
	}
	var lists []string
	var d int
	for {
		var list []string
		list, d = s.get(offset)
		lists = append(lists, list...)
		if d == 0 {
			break
		}
		offset = d
	}
	return lists
}
func (s *Msearch) Exist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.keyMap[key]
	if ok {
		return true
	}
	s.keyMap[key] = notExist
	return false
}

// empty 插入判断是否有空位，以及空位的长度.
func (s *Msearch) empty(offset int) (o int, start int, end int, t bool) {
	var lastDec int
	for {
		o, lastDec, start, end, t = s.empty1(offset)
		if lastDec == 0 || t {
			break
		}
		offset = lastDec
	}
	return
}

// getB8byOffset 这个offset是每个value的起始offset 得到最后的一个8位 offset只能有s.keyMap 获得。
func (s *Msearch) getB8byOffset(offset int) (b8 []byte) {
	var lastDec int
	for {
		lastDec, b8 = s.b8(offset)
		if lastDec == 0 {
			break
		}
		offset = lastDec
	}
	return
}

// 是否有空位，以及空位的长度
func (s *Msearch) empty1(offset int) (o int, lastDec int, start int, end int, t bool) {
	// t为false的时候 也就是没有空位 有b8
	var first bool
	total := bigUint64(s.bytesAddr[offset : offset+8])
	b := s.bytesAddr[offset : offset+total]
	o = offset
	for i := int(b[8] + 1 + 8); i < len(b[:len(b)-16]); {
		if b[i] == 0 {
			if !first {
				first = true
				t = true
				start = i
			}
			i++
			continue
		}
		if t {
			end = i
			return
		}
		i += int(b[i]) + 1
	}
	if t && end == 0 {
		end = total - 16
	}
	lastDec = bigUint64(b[total-8 : total])
	return
}

// 是否有空位，以及空位的长度
func (s *Msearch) b8(offset int) (lastDec int, b8 []byte) {
	// t为false的时候 也就是没有空位 有b8
	if offset >= s.offset {
		return 0, nil
	}
	total := bigUint64(s.bytesAddr[offset : offset+8])
	b8 = s.bytesAddr[offset+total-8 : offset+total]
	b := s.bytesAddr[offset : offset+total]
	lastDec = bigUint64(b[total-8 : total])
	return
}

func (s *Msearch) add(b8 []byte, key string, values ...string) (int, error) {
	var b = make([]byte, 1<<10)
	b[8] = byte(len(key))
	n := copy(b[9:], key)
	idx := n + 1 + 8
	for _, value := range values {
		if len(b) < idx+len(value)+2 {
			// 容量不足就扩容 扩容一定要覆盖下面的copy
			b = append(b, make([]byte, 1<<10)...)
		}
		// todo len(value)大于255？？
		if len(value) > 255 {
			return 0, errors.New("value exceed max length 255")
		}
		b[idx] = byte(len(value))
		// 一定要注意copy的地方
		copy(b[idx+1:], value)
		idx += 1 + len(value)
	}
	total := idx + 16
	binary.BigEndian.PutUint64(b[idx:], uint64(total+s.offset)) // todo 是否有必要？？
	b = b[:total]
	binary.BigEndian.PutUint64(b[:8], uint64(total))
	_, err := s.f.Write(b)
	if err != nil {
		return 0, err
	}
	if i, ok := s.keyMap[key]; !ok || i == notExist {
		s.keyMap[key] = s.offset

	}
	if len(b8) > 0 {
		// 末尾的
		binary.BigEndian.PutUint64(b8, uint64(s.offset))
	}
	s.offset += total
	return total, err
}

func (s *Msearch) del(offset int, valueMap map[string]struct{}) int {
	total := bigUint64(s.bytesAddr[offset : offset+8])
	if total == 0 {
		return 0
	}
	b := s.bytesAddr[offset : offset+total]
	for i := int(b[8] + 1 + 8); i < len(b[:len(b)-16]); {
		bi := int(b[i])
		if bi == 0 {
			i++
			continue
		}
		value := string(b[i+1 : i+1+int(b[i])])
		if _, ok := valueMap[value]; ok {
			copy(b[i+1:i+1+int(b[i])], make([]byte, int(b[i])))
			b[i] = 0
		}
		i += bi + 1

	}
	return bigUint64(b[total-8 : total])
}

func (s *Msearch) get(offset int) ([]string, int) {
	total := bigUint64(s.bytesAddr[offset : offset+8])
	b := s.bytesAddr[offset : offset+total]
	var list []string
	for i := int(b[8] + 1 + 8); i < len(b[:len(b)-16]); {
		if b[i] == 0 {
			i++
			continue
		}
		list = append(list, string(b[i+1:i+1+int(b[i])]))
		i += int(b[i]) + 1
	}
	lastDec := bigUint64(b[total-8 : total])
	return list, lastDec
}

// bigUint64 对大数字进行解码， binary.BigEndian.PutUint64 是编码.
func bigUint64(buf []byte) int {
	if len(buf) > 8 {
		// 内部使用方法，不应该出现如此错误。
		fmt.Println(buf, "error: 长度超过8位. len(buf)==>", len(buf))
		return 0
	}
	var x int
	for _, b := range buf {
		x = x<<8 | int(b)
	}
	return x
}