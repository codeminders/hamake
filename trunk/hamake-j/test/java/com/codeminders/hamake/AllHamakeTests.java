package com.codeminders.hamake;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses( { TestHamake.class, TestNoDepsExecutionGraph.class, TestDependencyExecutionGraph.class, TestSyntaxParser.class })
public class AllHamakeTests {}
