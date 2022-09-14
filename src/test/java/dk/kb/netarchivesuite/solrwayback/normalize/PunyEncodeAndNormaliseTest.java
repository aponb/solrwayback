package dk.kb.netarchivesuite.solrwayback.normalize;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import dk.kb.netarchivesuite.solrwayback.normalise.Normalisation;
import dk.kb.netarchivesuite.solrwayback.normalise.Normalisation.NormaliseType;
import dk.kb.netarchivesuite.solrwayback.properties.PropertiesLoader;
import org.junit.Before;
import org.junit.Test;

import dk.kb.netarchivesuite.solrwayback.UnitTestUtils;
import dk.kb.netarchivesuite.solrwayback.facade.Facade;
import dk.kb.netarchivesuite.solrwayback.service.dto.ArcEntry;


public class PunyEncodeAndNormaliseTest extends UnitTestUtils {


    @Test
    public void testPunyEncodingAndNormalize() throws Exception {
        
        Normalisation.setType(NormaliseType.NORMAL);
        String url="http://www.test.dk/ABC.cfm?value=27";        
        String urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
        assertEquals("http://test.dk/abc.cfm?value=27", urlPunyNorm);
        
        
        url="http://test.dk/abc.cfm?value=27&value2=Z.Y";
        urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
        assertEquals("http://test.dk/abc.cfm?value=27&value2=z.y", urlPunyNorm);
        
        
        url="http://pølser.dk/pølseguf.html?pølse=Medister";
        urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
        assertEquals("http://xn--plser-vua.dk/pølseguf.html?pølse=medister", urlPunyNorm);
        
        
        url="http://www.pølser.dk/pølseguf.html?pølse=Medister"; //normal normaliser removes www
        urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
        assertEquals("http://xn--plser-vua.dk/pølseguf.html?pølse=medister", urlPunyNorm);
        
        url="http://www.pølser.dk/"; //normal normaliser removes www 
        urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
        assertEquals("http://xn--plser-vua.dk/", urlPunyNorm);

        
    
    }
    @Test
    public void testPunyEncodingAndNormalizeWithLegacy() throws Exception {             
        Normalisation.setType(NormaliseType.LEGACY);
       
        String url="http://www.pølser.dk"; //normal should NOT remove www. 
        String urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
        assertEquals("http://www.xn--plser-vua.dk/", urlPunyNorm);

         url="http://www.pølser.dk/"; //normal should NOT remove www. 
         urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
         assertEquals("http://www.xn--plser-vua.dk/", urlPunyNorm);
        
         url="http://www.pølser.dk/pølseguf.html?pølse=ostepølse"; //legacy should remove www 
         urlPunyNorm= Facade.punyCodeAndNormaliseUrl(url);
         assertEquals("http://xn--plser-vua.dk/pølseguf.html?pølse=ostepølse", urlPunyNorm);
         
    }
    
    
   }
