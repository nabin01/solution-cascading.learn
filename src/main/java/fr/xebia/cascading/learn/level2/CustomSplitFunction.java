package fr.xebia.cascading.learn.level2;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class CustomSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
	private static final long serialVersionUID = 1L;
	
	public CustomSplitFunction(Fields wordField) {
		super(1, wordField);
	}

	@Override
	public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
			FunctionCall<Context> functionCall) {
		String line = functionCall.getArguments().getString(0);
		//String regex = "(?<!\\\\pL)(?=\\\\pL)[^ ]*(?<=\\\\pL)(?!\\\\pL)";
		//String regex = "s/\\s\\+/\\n/g";
		String[] word = line.split(" "); // or not ?
//		functionCall.getOutputCollector().add(new Tuple(word));
//		functionCall.getOutputCollector().add(new Tuple(word));
		for (String item : word) {
			functionCall.getOutputCollector().add(new Tuple(item));
		}
	}
}