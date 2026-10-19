// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

// NewDataHub is a constructor function type that creates and initializes a user-defined
// DataHub object using the provided DataAcc.
//
// A DataHub is typically a user-defined struct that embeds or wraps DataAcc and provides
// methods required by application logic functions.
type NewDataHub func(acc *DataAcc) any
