package com.codeminders.hamake;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.codeminders.hamake.dtr.TestSyntaxParser;

@RunWith(Suite.class)
@Suite.SuiteClasses( { TestHamake.class, TestNoDepsExecutionGraph.class, TestDependencyExecutionGraph.class, TestSyntaxParser.class })
public class AllHamakeTests {}
