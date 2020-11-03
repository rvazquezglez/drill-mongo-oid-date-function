package com.raul.udf;

import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;

@FunctionTemplate(
	name = "oidtodate",
	scope = FunctionTemplate.FunctionScope.SIMPLE,
	nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class OidToDateUdf implements org.apache.drill.exec.expr.DrillSimpleFunc {
	@Param
	org.apache.drill.exec.expr.holders.VarCharHolder in;

	// Using TimeStampHolder makes the dates go to 1979
	// So using IntHolder to combine with `TO_TIMESTAMP`
	// in the queries. like this:
	// TO_TIMESTAMP(oidtodate(p._id.`$oid`))
	@Output
	org.apache.drill.exec.expr.holders.IntHolder out;

	public void setup() {

	}

	public void eval() {
		String stringFromVarCharHolder = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(in);
		byte[] b = new byte[12];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) Integer.parseInt(
				stringFromVarCharHolder
					.substring(i * 2, i * 2 + 2), 16);
		}

		int outputValue = ((b[0]) << 24) |
			((b[1] & 0xff) << 16) |
			((b[2] & 0xff) << 8) |
			((b[3] & 0xff));

		out.value = outputValue;
	}
}
