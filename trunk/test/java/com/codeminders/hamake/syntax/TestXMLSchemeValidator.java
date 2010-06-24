package com.codeminders.hamake.syntax;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.HelperUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

import org.xml.sax.SAXException;
import org.junit.Test;
import org.junit.Assert;

public class TestXMLSchemeValidator {
    class TestSyntaxParser extends SyntaxParser
    {
        protected TestSyntaxParser(Context rootContext) {
            super(rootContext);
        }

        public void testValidate() throws IOException, SAXException {
            File localHamakeFile = HelperUtils.getHamakeTestResource("test-minimal.xml");
            InputStream in = new FileInputStream(localHamakeFile);
            try {
                Assert.assertTrue(validate(in)); 
            }
            finally {
                in.close();
            }
        }
    }

    @Test
    public void testValidator() throws IOException, SAXException {
        (new TestSyntaxParser(null)).testValidate();
    }
}
