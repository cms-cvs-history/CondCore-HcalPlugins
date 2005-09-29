
/*----------------------------------------------------------------------

Toy EDProducers and EDProducts for testing purposes only.

----------------------------------------------------------------------*/

#include <stdexcept>
#include <string>
#include <iostream>
#include <map>
#include "FWCore/Framework/interface/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "FWCore/Framework/interface/MakerMacros.h"

#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

#include "CondFormats/HcalObjects/interface/HcalPedestals.h"
#include "CondFormats/HcalObjects/interface/HcalPedestalWidths.h"
#include "CondFormats/HcalObjects/interface/HcalGains.h"
#include "CondFormats/HcalObjects/interface/HcalGainWidths.h"
#include "DataFormats/HcalDetId/interface/HcalDetId.h"
#include "CondFormats/DataRecord/interface/HcalPedestalsRcd.h"
#include "CondFormats/DataRecord/interface/HcalPedestalWidthsRcd.h"
#include "CondFormats/DataRecord/interface/HcalGainsRcd.h"
#include "CondFormats/DataRecord/interface/HcalGainWidthsRcd.h"

using namespace std;

namespace edmtest
{
  class HcalCalibrationsAnalizer : public edm::EDAnalyzer
  {
  public:
    explicit  HcalCalibrationsAnalizer(edm::ParameterSet const& p) 
    { }
    explicit  HcalCalibrationsAnalizer(int i) 
    { }
    virtual ~ HcalCalibrationsAnalizer() { }
    virtual void analyze(const edm::Event& e, const edm::EventSetup& c);
  private:
  };
  
  void
   HcalCalibrationsAnalizer::analyze(const edm::Event& e, const edm::EventSetup& context)
  {
    using namespace edm::eventsetup;
    // Context is not used.
    std::cout <<"HcalCalibrationsAnalizer::analyze-> I AM IN RUN NUMBER "<<e.id().run() <<std::endl;
    std::cout <<"HcalCalibrationsAnalizer::analyze->  ---EVENT NUMBER "<<e.id().run() <<std::endl;
    edm::eventsetup::ESHandle<HcalPedestals> pPeds;
    context.get<HcalPedestalsRcd>().get(pPeds);
    edm::eventsetup::ESHandle<HcalPedestalWidths> pPedWs;
    context.get<HcalPedestalWidthsRcd>().get(pPedWs);
    edm::eventsetup::ESHandle<HcalGains> pGains;
    context.get<HcalGainsRcd>().get(pGains);
    edm::eventsetup::ESHandle<HcalGainWidths> pGainWs;
    context.get<HcalGainWidthsRcd>().get(pGainWs);
    //call tracker code
    //
    std::cout <<" Hcal peds for channel HB eta=15, phi=5, depth=2 "<<std::endl;
    int channelID = cms::HcalDetId (cms::HcalBarrel, 15, 5, 2).rawId();
    const HcalPedestals* myped=pPeds.product();
    const HcalPedestalWidths* mypedW=pPedWs.product();
    const HcalGains* mygain=pGains.product();
    const HcalGainWidths* mygainW=pGainWs.product();

    const float* values = myped->getValues (channelID);
    if (values) std::cout << "pedestals for channel " << channelID << ": "
			  << values [0] << '/' << values [1] << '/' << values [2] << '/' << values [3] << std::endl; 
    values = mypedW->getValues (channelID);
    if (values) std::cout << "pedestal widths for channel " << channelID << ": "
			  << values [0] << '/' << values [1] << '/' << values [2] << '/' << values [3] << std::endl; 
    values = mygain->getValues (channelID);
    if (values) std::cout << "gains for channel " << channelID << ": "
			  << values [0] << '/' << values [1] << '/' << values [2] << '/' << values [3] << std::endl; 
    values = mygainW->getValues (channelID);
    if (values) std::cout << "gain widts for channel " << channelID << ": "
			  << values [0] << '/' << values [1] << '/' << values [2] << '/' << values [3] << std::endl; 
  }
  DEFINE_FWK_MODULE(HcalCalibrationsAnalizer)
}
