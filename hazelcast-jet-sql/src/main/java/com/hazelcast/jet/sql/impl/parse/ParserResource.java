/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.parse;

import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.Resources.BaseMessage;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.validate.SqlValidatorException;

public interface ParserResource {

    ParserResource RESOURCE = Resources.create(ParserResource.class);

    @BaseMessage("{0}")
    ExInst<SqlValidatorException> error(String s);

    @BaseMessage("{0} is not supported")
    ExInst<SqlValidatorException> notSupported(String name);

    @BaseMessage("{0} is not supported with {1}")
    ExInst<SqlValidatorException> notSupported(String option, String statement);

    @BaseMessage("Too many columns were specified")
    ExInst<SqlValidatorException> tooManyColumnsSpecified();

    @BaseMessage("Column ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateColumn(String columnName);

    @BaseMessage("Missing column type")
    ExInst<SqlValidatorException> missingColumnType();

    @BaseMessage("Unexpected column type")
    ExInst<SqlValidatorException> columnTypeSpecified();

    @BaseMessage("Invalid external name ''{0}'', nested fields are not supported")
    ExInst<SqlValidatorException> nestedField(String externalColumnName);

    @BaseMessage("Option ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateOption(String optionName);
}
