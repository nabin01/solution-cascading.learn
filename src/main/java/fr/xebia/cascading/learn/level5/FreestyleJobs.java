package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.*;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import javafx.scene.chart.PieChart;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {

	/**
	 * Word count is the Hadoop "Hello world" so it should be the first step.
	 * <p>
	 * source field(s) : "line"
	 * sink field(s) : "word","count"
	 */
	public static FlowDef countWordOccurences(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		Pipe assembly = new Pipe("assembly");
//		assembly = new Each(assembly, new Fields("line"), new RegexSplitGenerator(new Fields("word"),"[^A-Za-z\\-]"));
//		assembly = new Each(assembly, new ExpressionFilter("word.length() == 0", String.class));
//		assembly = new Each(assembly, new Fields("line"), new RegexGenerator(new Fields("word"), "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)"));
		assembly = new Each(assembly, new Fields("line"), new RegexSplitGenerator(new Fields("word"), "[\\W]+"));
//		assembly = new Each(assembly, new Fields("line"), new RegexSplitGenerator(new Fields("word"), "[ \\[\\]\\(\\),.]"));

		assembly = new Each(assembly, new ExpressionFunction(new Fields("word"),"word.toLowerCase()", String.class));

		// group by and count
		Pipe groupBy = new GroupBy(assembly);
		Pipe count = new Every(groupBy, new Count(new Fields("count")));

		return FlowDef.flowDef().addSource(assembly, source).addTailSink(count, sink);
	}
	
	/**
	 * Now, let's try a non trivial job : td-idf. Assume that each line is a
	 * document.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "docId","tfidf","word"
	 * 
	 * <pre>
	 * t being a term
	 * t' being any other term
	 * d being a document
	 * D being the set of documents
	 * Dt being the set of documents containing the term t
	 * 
	 * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
	 * 
	 * where
	 * 
	 * tf(t,d) = f(t,d) / max (f(t',d))
	 * ie the frequency of the term divided by the highest term frequency for the same document
	 * 
	 * idf(t, D) = log( size(D) / size(Dt) )
	 * ie the logarithm of the number of documents divided by the number of documents containing the term t 
	 * </pre>
	 * 
	 * Wikipedia provides the full explanation
	 * @see http://en.wikipedia.org/wiki/tf-idf
	 * 
	 * If you are having issue applying functions, you might need to learn about field algebra
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch04-tuple-fields.html#field-algebra
	 * 
	 * {@link First} could be useful for isolating the maximum.
	 * 
	 * {@link HashJoin} can allow to do cross join.
	 * 
	 * PS : Do no think about efficiency, at least, not for a first try.
	 * PPS : You can remove results where tfidf < 0.1
	 */
	public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		Pipe assembly = new Pipe("assembly");
		Pipe temp = new Pipe("temp");

		Pipe docId = new Retain(assembly, new Fields("id"));
		Pipe doc = new Retain(temp, new Fields("content"));
//		doc = new
		Pipe join = new HashJoin(docId, new Fields("id"), doc, new Fields("content"), new InnerJoin());

		return FlowDef.flowDef()
				.addSource(assembly, source)
				.addSource(temp, source)
				.addTailSink(doc, sink);
	}
}
