import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.bson.types.ObjectId;

import static org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope.SIMPLE;
import static org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling.NULL_IF_NULL;
import static org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder;

@FunctionTemplate(
	name = "oidtodate",
	scope = SIMPLE,
	nulls = NULL_IF_NULL
)
public class OidToDateUdf implements DrillSimpleFunc {
	@Param
	VarCharHolder in;

	@Output
	TimeStampHolder out;

	public void setup() {

	}

	public void eval() {
		out.value = new ObjectId(
			getStringFromVarCharHolder(in)
		).getTimestamp();
	}
}
