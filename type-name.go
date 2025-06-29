// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"fmt"
)

func typeNameOf(v any) string {
	return fmt.Sprintf("%T", v)
}

func typeNameOfTypeParam[T any]() string {
	return fmt.Sprintf("%T", new(T))[1:]
}
