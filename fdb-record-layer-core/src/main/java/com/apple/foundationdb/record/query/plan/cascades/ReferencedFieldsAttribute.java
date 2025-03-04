/*
 * ReferencedFieldsAttribute.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * An attribute giving a set of referenced field value.
 */
public class ReferencedFieldsAttribute implements PlannerAttribute<ReferencedFieldsAttribute.ReferencedFields> {
    public static final PlannerAttribute<ReferencedFields> REFERENCED_FIELDS = new ReferencedFieldsAttribute();

    /**
     * A set of referenced field values.
     */
    public static class ReferencedFields {
        @Nonnull
        private final Set<FieldValue> referencedFieldValues;

        public ReferencedFields(@Nonnull final Set<FieldValue> referencedFieldValues) {
            this.referencedFieldValues = referencedFieldValues;
        }

        @Nonnull
        public Set<FieldValue> getReferencedFieldValues() {
            return referencedFieldValues;
        }
    }

    @Nonnull
    @Override
    public Optional<ReferencedFields> combine(@Nonnull final ReferencedFields currentProperty, @Nonnull final ReferencedFields newProperty) {
        final ImmutableSet<FieldValue> referencedFields =
                ImmutableSet.<FieldValue>builder()
                        .addAll(currentProperty.getReferencedFieldValues())
                        .addAll(newProperty.getReferencedFieldValues())
                        .build();

        if (referencedFields.size() > currentProperty.getReferencedFieldValues().size()) {
            return Optional.of(new ReferencedFields(referencedFields));
        }

        return Optional.empty();
    }
}
