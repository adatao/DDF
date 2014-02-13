package com.adatao.ddf.content;


/**
 * Unit tests for generic DDF.
 */
/*
public class ARepresentationHandlerTests {
  public static class Helper extends ADDFManager {
    public Helper(DDF ddf) {
      super(ddf);
      this.setRepresentationHandler(new Handler(this));
    }

    @Override
    protected IComputeBasicStatistics createBasicStatisticsComputer() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleIndexing createIndexingHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleJoins createJoinsHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleMetaData createMetaDataHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleMiscellany createMiscellanyHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleMissingData createMissingDataHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleMutability createMutabilityHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandlePersistence createPersistenceHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleRepresentations createRepresentationHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleReshaping createReshapingHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleSchema createSchemaHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleStreamingData createStreamingDataHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleTimeSeries createTimeSeriesHandler() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IRunAlgorithms createAlgorithmRunner() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected IHandleViews createViewHandler() {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class Handler extends ARepresentationHandler {
    public Handler(ADDFManager container) {
      super(container);
    }

    @Override
    public void cacheAll() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void uncacheAll() {
      // TODO Auto-generated method stub
      
    }
  }

  private static DDF ddf = new DDF(new Helper(null));
  private static ADDFManager helper = ddf.getHelper();
  private static IHandleRepresentations handler = helper.getRepresentationHandler();

  private static List<String> list = new ArrayList<String>();


  @BeforeClass
  public static void setupFixture() {
    Assert.assertNotNull("Newly instantiated DDF from RDD should not be null", ddf);

    list.add("a");
    list.add("b");
    list.add("c");
  }

  @AfterClass
  public static void shutdownFixture() {
  }


  @Test
  public void testRepresentDDF() {
    handler.reset();
    Assert.assertNull("There should not be any existing representations", handler.get(list.get(0).getClass()));

    handler.set(list, list.get(0).getClass());
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(list.get(0).getClass()));

    handler.add(list, list.get(0).getClass());
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(list.get(0).getClass()));

    handler.remove(list.get(0).getClass());
    Assert.assertNull("There should now be no representation of type <List,String>",
        handler.get(list.get(0).getClass()));
  }
}
*/
