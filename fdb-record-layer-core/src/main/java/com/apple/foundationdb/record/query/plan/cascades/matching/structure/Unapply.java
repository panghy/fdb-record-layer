/*
 * Unapply.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import javax.annotation.Nonnull;

/**
 * Interface for the unapply function using in a variety of matchers.
 * @param <T> the type that is extracted from
 * @param <U> the type that is extracted into
 */
@FunctionalInterface
public interface Unapply<T, U> {
    /**
     * Unapplies this function to the given argument.
     *
     * @param t the function argument
     * @return the function result
     */
    @Nonnull
    U unapply(@Nonnull T t);
}
