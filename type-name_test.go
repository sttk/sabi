package sabi

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type V struct {
	A string
	B int
}

func (v V) GetA() string { return v.A }
func (v V) GetB() int    { return v.B }

type I interface {
	GetA() string
	GetB() int
}

func TestTypeNameOf_values(t *testing.T) {
	b := true
	n := 123
	n8 := int8(123)
	n16 := int16(123)
	n32 := int32(123)
	n64 := int64(123)
	u := uint(123)
	u8 := uint8(123)
	u16 := uint16(123)
	u32 := uint32(123)
	u64 := uint64(123)
	f32 := float32(1.23)
	f64 := float64(1.23)
	c64 := complex64(1 + 2i)
	c128 := complex128(1 + 2i)
	str := "ABC"
	arr := [2]int{1, 2}
	slc := arr[:]
	objV := V{A: "a", B: 1}
	ptrV := &objV
	var if1 I = objV
	var if2 I = &objV
	m := make(map[string]string)
	fn := func() {}
	chN := make(chan int)
	chA := make(chan V)

	assert.Equal(t, typeNameOf(b), "bool")
	assert.Equal(t, typeNameOf(n), "int")
	assert.Equal(t, typeNameOf(n8), "int8")
	assert.Equal(t, typeNameOf(n16), "int16")
	assert.Equal(t, typeNameOf(n32), "int32")
	assert.Equal(t, typeNameOf(n64), "int64")
	assert.Equal(t, typeNameOf(u), "uint")
	assert.Equal(t, typeNameOf(u8), "uint8")
	assert.Equal(t, typeNameOf(u16), "uint16")
	assert.Equal(t, typeNameOf(u32), "uint32")
	assert.Equal(t, typeNameOf(u64), "uint64")
	assert.Equal(t, typeNameOf(f32), "float32")
	assert.Equal(t, typeNameOf(f64), "float64")
	assert.Equal(t, typeNameOf(c64), "complex64")
	assert.Equal(t, typeNameOf(c128), "complex128")
	assert.Equal(t, typeNameOf(str), "string")
	assert.Equal(t, typeNameOf(arr), "[2]int")
	assert.Equal(t, typeNameOf(slc), "[]int")
	assert.Equal(t, typeNameOf(objV), "sabi.V")
	assert.Equal(t, typeNameOf(ptrV), "*sabi.V")
	assert.Equal(t, typeNameOf(if1), "sabi.V")
	assert.Equal(t, typeNameOf(if2), "*sabi.V")
	assert.Equal(t, typeNameOf(m), "map[string]string")
	assert.Equal(t, typeNameOf(fn), "func()")
	assert.Equal(t, typeNameOf(chN), "chan int")
	assert.Equal(t, typeNameOf(chA), "chan sabi.V")
}

func TestTypeNameOf_pointers(t *testing.T) {
	b := true
	n := 123
	n8 := int8(123)
	n16 := int16(123)
	n32 := int32(123)
	n64 := int64(123)
	u := uint(123)
	u8 := uint8(123)
	u16 := uint16(123)
	u32 := uint32(123)
	u64 := uint64(123)
	f32 := float32(1.23)
	f64 := float64(1.23)
	c64 := complex64(1 + 2i)
	c128 := complex128(1 + 2i)
	str := "ABC"
	arr := [2]int{1, 2}
	slc := arr[:]
	objV := V{A: "a", B: 1}
	ptrV := &objV
	var if1 I = objV
	var if2 I = &objV
	m := make(map[string]string)
	fn := func() {}
	chN := make(chan int)
	chA := make(chan V)

	assert.Equal(t, typeNameOf(&b), "*bool")
	assert.Equal(t, typeNameOf(&n), "*int")
	assert.Equal(t, typeNameOf(&n8), "*int8")
	assert.Equal(t, typeNameOf(&n16), "*int16")
	assert.Equal(t, typeNameOf(&n32), "*int32")
	assert.Equal(t, typeNameOf(&n64), "*int64")
	assert.Equal(t, typeNameOf(&u), "*uint")
	assert.Equal(t, typeNameOf(&u8), "*uint8")
	assert.Equal(t, typeNameOf(&u16), "*uint16")
	assert.Equal(t, typeNameOf(&u32), "*uint32")
	assert.Equal(t, typeNameOf(&u64), "*uint64")
	assert.Equal(t, typeNameOf(&f32), "*float32")
	assert.Equal(t, typeNameOf(&f64), "*float64")
	assert.Equal(t, typeNameOf(&c64), "*complex64")
	assert.Equal(t, typeNameOf(&c128), "*complex128")
	assert.Equal(t, typeNameOf(&str), "*string")
	assert.Equal(t, typeNameOf(&arr), "*[2]int")
	assert.Equal(t, typeNameOf(&slc), "*[]int")
	assert.Equal(t, typeNameOf(&objV), "*sabi.V")
	assert.Equal(t, typeNameOf(&ptrV), "**sabi.V")
	assert.Equal(t, typeNameOf(&if1), "*sabi.I")
	assert.Equal(t, typeNameOf(&if2), "*sabi.I")
	assert.Equal(t, typeNameOf(&m), "*map[string]string")
	assert.Equal(t, typeNameOf(&fn), "*func()")
	assert.Equal(t, typeNameOf(&chN), "*chan int")
	assert.Equal(t, typeNameOf(&chA), "*chan sabi.V")
}

func TestTypeOfTypeParam_value(t *testing.T) {
	assert.Equal(t, typeNameOfTypeParam[bool](), "bool")
	assert.Equal(t, typeNameOfTypeParam[int](), "int")
	assert.Equal(t, typeNameOfTypeParam[int8](), "int8")
	assert.Equal(t, typeNameOfTypeParam[int16](), "int16")
	assert.Equal(t, typeNameOfTypeParam[int32](), "int32")
	assert.Equal(t, typeNameOfTypeParam[int64](), "int64")
	assert.Equal(t, typeNameOfTypeParam[uint](), "uint")
	assert.Equal(t, typeNameOfTypeParam[uint8](), "uint8")
	assert.Equal(t, typeNameOfTypeParam[uint16](), "uint16")
	assert.Equal(t, typeNameOfTypeParam[uint32](), "uint32")
	assert.Equal(t, typeNameOfTypeParam[uint64](), "uint64")
	assert.Equal(t, typeNameOfTypeParam[float32](), "float32")
	assert.Equal(t, typeNameOfTypeParam[float64](), "float64")
	assert.Equal(t, typeNameOfTypeParam[complex64](), "complex64")
	assert.Equal(t, typeNameOfTypeParam[complex128](), "complex128")
	assert.Equal(t, typeNameOfTypeParam[string](), "string")
	assert.Equal(t, typeNameOfTypeParam[[2]int](), "[2]int")
	assert.Equal(t, typeNameOfTypeParam[[]int](), "[]int")
	assert.Equal(t, typeNameOfTypeParam[V](), "sabi.V")
	assert.Equal(t, typeNameOfTypeParam[*V](), "*sabi.V")
	assert.Equal(t, typeNameOfTypeParam[I](), "sabi.I")
	assert.Equal(t, typeNameOfTypeParam[map[string]string](), "map[string]string")
	assert.Equal(t, typeNameOfTypeParam[func()](), "func()")
	assert.Equal(t, typeNameOfTypeParam[chan int](), "chan int")
	assert.Equal(t, typeNameOfTypeParam[chan V](), "chan sabi.V")
}

func TestTypeOfTypeParam_pointer(t *testing.T) {
	assert.Equal(t, typeNameOfTypeParam[*bool](), "*bool")
	assert.Equal(t, typeNameOfTypeParam[*int](), "*int")
	assert.Equal(t, typeNameOfTypeParam[*int8](), "*int8")
	assert.Equal(t, typeNameOfTypeParam[*int16](), "*int16")
	assert.Equal(t, typeNameOfTypeParam[*int32](), "*int32")
	assert.Equal(t, typeNameOfTypeParam[*int64](), "*int64")
	assert.Equal(t, typeNameOfTypeParam[*uint](), "*uint")
	assert.Equal(t, typeNameOfTypeParam[*uint8](), "*uint8")
	assert.Equal(t, typeNameOfTypeParam[*uint16](), "*uint16")
	assert.Equal(t, typeNameOfTypeParam[*uint32](), "*uint32")
	assert.Equal(t, typeNameOfTypeParam[*uint64](), "*uint64")
	assert.Equal(t, typeNameOfTypeParam[*float32](), "*float32")
	assert.Equal(t, typeNameOfTypeParam[*float64](), "*float64")
	assert.Equal(t, typeNameOfTypeParam[*complex64](), "*complex64")
	assert.Equal(t, typeNameOfTypeParam[*complex128](), "*complex128")
	assert.Equal(t, typeNameOfTypeParam[*string](), "*string")
	assert.Equal(t, typeNameOfTypeParam[*[2]int](), "*[2]int")
	assert.Equal(t, typeNameOfTypeParam[*[]int](), "*[]int")
	assert.Equal(t, typeNameOfTypeParam[*V](), "*sabi.V")
	assert.Equal(t, typeNameOfTypeParam[**V](), "**sabi.V")
	assert.Equal(t, typeNameOfTypeParam[*I](), "*sabi.I")
	assert.Equal(t, typeNameOfTypeParam[*map[string]string](), "*map[string]string")
	assert.Equal(t, typeNameOfTypeParam[*func()](), "*func()")
	assert.Equal(t, typeNameOfTypeParam[*chan int](), "*chan int")
	assert.Equal(t, typeNameOfTypeParam[*chan V](), "*chan sabi.V")
}
