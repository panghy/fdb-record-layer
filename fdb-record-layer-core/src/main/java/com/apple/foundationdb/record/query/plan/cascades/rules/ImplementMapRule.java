/*
 * ImplementMapRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that implements a select expression without predicates over a single {@link RecordQueryPlan} as a
 * {@link RecordQueryMapPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementMapRule extends PlannerRule<SelectExpression> {
    private static final CollectionMatcher<RecordQueryPlan> innerPlansMatcher = some(RecordQueryPlanMatchers.anyPlan());
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverPlans(innerPlansMatcher);
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(CollectionMatcher.empty(), exactly(innerQuantifierMatcher));

    public ImplementMapRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final SelectExpression selectExpression = bindings.get(root);
        final Collection<? extends RecordQueryPlan> innerPlans = bindings.get(innerPlansMatcher);
        if (innerPlans.isEmpty()) {
            return;
        }

        final Quantifier.ForEach innerQuantifier = bindings.get(innerQuantifierMatcher);
        final var resultValue = selectExpression.getResultValue();
        if (resultValue instanceof QuantifiedObjectValue &&
                ((QuantifiedObjectValue)resultValue).getAlias().equals(innerQuantifier.getAlias())) {
            return;
        }
        
        final GroupExpressionRef<? extends RecordQueryPlan> referenceOverPlans = GroupExpressionRef.from(innerPlans);

        call.yield(GroupExpressionRef.of(
                new RecordQueryMapPlan(
                        Quantifier.physicalBuilder()
                                .morphFrom(innerQuantifier)
                                .build(referenceOverPlans),
                        resultValue)));
    }
}
