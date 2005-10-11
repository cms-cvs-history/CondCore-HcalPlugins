#include <stdlib.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>

// other
#include "DataFormats/HcalDetId/interface/HcalDetId.h"

// pool
#include "PluginManager/PluginManager.h"
#include "POOLCore/POOLContext.h"
#include "POOLCore/Token.h"
#include "FileCatalog/URIParser.h"
#include "FileCatalog/IFileCatalog.h"
#include "StorageSvc/DbType.h"
#include "PersistencySvc/DatabaseConnectionPolicy.h"
#include "PersistencySvc/ISession.h"
#include "PersistencySvc/ITransaction.h"
#include "PersistencySvc/IDatabase.h"
#include "PersistencySvc/Placement.h"
#include "DataSvc/DataSvcFactory.h"
#include "DataSvc/IDataSvc.h"
#include "DataSvc/ICacheSvc.h"
#include "DataSvc/Ref.h"
#include "RelationalAccess/RelationalException.h"
 
// conditions
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"

// Hcal calibrations
#include "CalibCalorimetry/HcalAlgos/interface/HcalDbServiceHardcode.h"
#include "CondFormats/HcalObjects/interface/HcalPedestals.h"
#include "CondFormats/HcalObjects/interface/HcalPedestalWidths.h"
#include "CondFormats/HcalObjects/interface/HcalGains.h"
#include "CondFormats/HcalObjects/interface/HcalGainWidths.h"

using namespace cms;

namespace {

  pool::Ref <HcalPedestals> gPedestals;
  pool::Ref <HcalGains> gGains;

  class Args {
    private:
    std::string mProgramName;
    std::vector <std::string> mOptions;
    std::vector <std::string> mParameters;
    std::vector <std::string> mArgs;
    std::map <std::string, std::string> mParsed;
    std::map <std::string, std::string> mComments;
  public:
    Args () {};
    ~Args () {};
    
    void defineOption (const std::string& fOption, const std::string& fComment = "") {
      mOptions.push_back (fOption);
      mComments [fOption] = fComment;
    }
    
    void defineParameter (const std::string& fParameter, const std::string& fComment = "") {
      mParameters.push_back (fParameter);
      mComments [fParameter] = fComment;
    }
    
    void parse (int nArgs, char* fArgs []) {
      if (nArgs <= 0) return;
      mProgramName = std::string (fArgs [0]);
      int iarg = 0;
      while (++iarg < nArgs) {
	std::string arg (fArgs [iarg]);
	if (arg [0] != '-') mArgs.push_back (arg);
	else {
	  if (std::find (mOptions.begin(), mOptions.end (), arg) !=  mOptions.end ()) {
	    mParsed [arg] = "";
	  }
	  if (std::find (mParameters.begin(), mParameters.end (), arg) !=  mParameters.end ()) {
	    if (iarg >= nArgs) {
	      std::cerr << "ERROR: Parameter " << arg << " has no value specified. Ignore parameter." << std::endl;
	    }
	    else {
	      mParsed [arg] = std::string (fArgs [++iarg]);
	    }
	  }
	}
      }
    }

    void printOptionsHelp () const {
      char buffer [1024];
      std::cout << "Parameters:" << std::endl;
      for (unsigned i = 0; i < mParameters.size (); i++) {
	std::map<std::string, std::string>::const_iterator it = mComments.find (mParameters [i]);
	std::string comment = it != mComments.end () ? it->second : "uncommented";
	sprintf (buffer, "  %-8s <value> : %s", (mParameters [i]).c_str(),  comment.c_str());
	std::cout << buffer << std::endl;
      }
      std::cout << "Options:" << std::endl;
      for (unsigned i = 0; i < mOptions.size (); i++) {
	std::map<std::string, std::string>::const_iterator it = mComments.find (mOptions [i]);
	std::string comment = it != mComments.end () ? it->second : "uncommented";
	sprintf (buffer, "  %-8s <value> : %s", (mOptions [i]).c_str(),  comment.c_str());
	std::cout << buffer << std::endl;
      }
    }
    
    std::string command () const {
      int ipos = mProgramName.rfind ('/');
      return std::string (mProgramName, ipos+1);
    }
    
    std::vector<std::string> arguments () const {return mArgs;}
    
    bool optionIsSet (const std::string& fOption) const {
      return mParsed.find (fOption) != mParsed.end ();
    }
    
    std::string getParameter (const std::string& fKey) {
      if (optionIsSet (fKey)) return mParsed [fKey];
      return "";
    }
  };
  
  class PoolData {
  public:
    PoolData (const std::string& fConnect) 
      : mConnect (fConnect) {
	std::cout << "PoolData-> Using DB connection: " << fConnect << std::endl; 
	seal::PluginManager::get()->initialise();
	pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
	pool::POOLContext::loadComponent( "POOL/Services/EnvironmentAuthenticationService" );
	
	pool::URIParser parser;
	parser.parse();
	
	//	mCatalog.setWriteCatalog (parser.contactstring ());
	mCatalog.setWriteCatalog(parser.contactstring());
	mCatalog.connect();
	mCatalog.start();
	mService = pool::DataSvcFactory::create (&mCatalog);
	
	pool::DatabaseConnectionPolicy policy;  
	policy.setWriteModeForNonExisting(pool::DatabaseConnectionPolicy::CREATE);
	policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::UPDATE); 
	mService->session().setDefaultConnectionPolicy(policy);
	mPlacement.setDatabase(mConnect, pool::DatabaseSpecification::PFN); 
	mPlacement.setTechnology(pool::POOL_RDBMS_StorageType.type());
      }
    
    ~PoolData () {
      mService->session().disconnectAll();
      mCatalog.commit ();
      mCatalog.disconnect ();
      delete mService;
    }

    template <class T>
    bool StoreData (T* fObject, unsigned fMaxRun, const std::string& fTag, const std::string& fContainer) {
      pool::Ref <T> ref (mService, fObject);
      pool::Ref <cond::IOV> iovref (mService, new cond::IOV);
      try {
	mService->transaction().start(pool::ITransaction::UPDATE);
	mPlacement.setContainerName (fContainer);
	ref.markWrite (mPlacement);
	// IOV business
	unsigned maxRun = fMaxRun == 0 ? 0xffffffff : fMaxRun;
	iovref->iov.insert (std::make_pair (maxRun, ref.toString ()));
	mPlacement.setContainerName ("IOV");
	iovref.markWrite (mPlacement);
	mService->transaction().commit();
      }
      catch( const seal::Exception& e){
	std::cerr << "seal error: " << e.what() << std::endl; 
	return false;
      }
      catch ( ... ) {
	std::cerr << " other StoreDatae error "  << std::endl; 
	return false;
      }
      // metadata
      cond::MetaData metadata (connect ());
      metadata.addMapping (fTag, iovref.toString ());
      return true;
    }
  

    
    pool::IDataSvc* service () {return  mService;}
    std::string connect () {return mConnect;}
    
  private:
    std::string mConnect;
    pool::IFileCatalog mCatalog;
    pool::IDataSvc* mService;
    pool::Placement mPlacement;
  };
  
  std::string getToken (PoolData& fDB,  const std::string& fTag, unsigned fRun) {
    cond::MetaData md (fDB.connect ());
    std::string iovToken = md.getToken (fTag);
    if (iovToken.empty ()) {
      std::cout << "No IOV token for tag " << fTag << std::endl;
      return "";
    }
    try {
      pool::Ref<cond::IOV> iov(fDB.service (), iovToken);
      // scan IOV, search for valid data
      for (std::map<unsigned long,std::string>::iterator iovi = iov->iov.begin (); iovi != iov->iov.end (); iovi++) {
	if (fRun <= iovi->first) return iovi->second; 
      }
    }
    catch( const pool::RelationalTableNotFound& e ){
      std::cerr << "getToken-> pool::RelationalTableNotFound Exception" << std::endl;
      return "";
    }
    catch (const seal::Exception& e) {
      std::cerr<<"getToken-> seal exception: " << e.what() << std::endl;
      return "";
    }
    catch(...){
      std::cerr << "getToken-> Funny error" << std::endl;
    }
    std::cerr << "getToken-> no object for run " << fRun << " is found" << std::endl;
    return "";
  }
  
template <class T>
 pool::Ref<T> getDataFromDb (PoolData& fDB, const std::string& fTag, unsigned fRun) {
  pool::Ref<T> result;
    fDB.service ()->transaction().start(pool::ITransaction::READ);
    std::string token = getToken (fDB, fTag, fRun);
    if (!token.empty ()) {
      try {
	result = pool::Ref <T> (fDB.service (), token);
      }
      catch( const pool::RelationalTableNotFound& e ){
	std::cerr << "getDataFromDb-> pool::RelationalTableNotFound Exception" << std::endl;
      }
      catch (const seal::Exception& e) {
	std::cerr<<"getDataFromDb-> seal exception: " << e.what() << std::endl;
      }
      catch(...){
	std::cerr << "getDataFromDb-> Funny error" << std::endl;
      }
    }
    return result;
  }

  template <class T> 
  void dumpData (pool::Ref<T>& fObject, const std::string& fOtput) {
    std::ofstream out (fOtput.c_str());
    char buffer [1024];
    sprintf (buffer, "# %4s %4s %4s %4s %8s %8s %8s %8s %10s\n", "eta", "phi", "dep", "det", "cap1", "cap2", "cap3", "cap4", "HcalDetId");
    out << buffer;
    std::vector<unsigned long> channels = fObject->getAllChannels ();
    for (std::vector<unsigned long>::iterator channel = channels.begin ();
	 channel !=  channels.end ();
	 channel++) {
      HcalDetId id ((uint32_t) *channel);
      const float* values = fObject->getValues (*channel);
      std::string subdet = "HB";
      if (id.subdet() == HcalEndcap) subdet = "HE";
      else if (id.subdet() == HcalForward) subdet = "HF";
      if (values) {
	sprintf (buffer, "  %4i %4i %4i %4s %8.5f %8.5f %8.5f %8.5f %10X\n",
		 id.ieta(), id.iphi(), id.depth (), subdet.c_str (), values[0], values[1], values[2], values[3], (unsigned)*channel);
	out << buffer;
      }
    }
  }

  std::vector <std::string> splitString (const std::string& fLine) {
    std::vector <std::string> result;
    int start = 0;
    bool empty = true;
    for (unsigned i = 0; i < fLine.size (); i++) {
      if (fLine [i] == ' ') {
	if (!empty) {
	  std::string item (fLine, start, i-start);
	  result.push_back (item);
	  empty = true;
	}
	start = i+1;
      }
      else {
	if (empty) empty = false;
      }
    }
    return result;
  }

template <class T>
 void readData (const std::string& fInput, T* fObject) {
    char buffer [1024];

    std::ifstream in (fInput.c_str());
    while (in.getline(buffer, 1024)) {
      if (buffer [0] == '#') continue; //ignore comment
      std::vector <std::string> items = splitString (std::string (buffer));
      if (items.size () < 8) {
	std::cerr << "Bad line: " << buffer << "\n line must contain 8 items: eta, phi, depth, subdet, 4xvalues" << std::endl;
	continue;
      }
      int eta = atoi (items [0].c_str());
      int phi = atoi (items [1].c_str());
      int depth = atoi (items [2].c_str());
      HcalSubdetector subdet = HcalBarrel;
      if (items [3] == "HE") subdet = HcalEndcap;
      else if (items [3] == "HF") subdet = HcalForward;
      HcalDetId id (subdet, eta, phi, depth);

      fObject->addValue (id.rawId(), 
			 atof (items [4].c_str()), atof (items [5].c_str()), 
			 atof (items [6].c_str()), atof (items [7].c_str()));
    }
    fObject->sort ();
  }



  void printHelp (const Args& args) {
    char buffer [1024];
    std::cout << "Tool to manipulate by Hcal Calibrations" << std::endl;
    std::cout << "    feedback -> ratnikov@fnal.gov" << std::endl;
    std::cout << "Use:" << std::endl;
    sprintf (buffer, " %s dump <what> <options> <parameters>\n", args.command ().c_str());
    std::cout << buffer;
    sprintf (buffer, " %s fill <what> <options> <parameters>\n", args.command ().c_str());
    std::cout << buffer;
    std::cout << "  where <what> is: \n    pedestals\n    gains" << std::endl;
    args.printOptionsHelp ();
  }

}

int main (int argn, char* argv []) {

  Args args;
  args.defineParameter ("-connect", "DB connection string, POOL format");
  args.defineParameter ("-input", "input file where to get constants from");
  args.defineParameter ("-output", "output file where to dump constants to");
  args.defineParameter ("-run", "run # for which constands should be dumped");
  args.defineParameter ("-tag", "tag for the constants set");
  args.defineOption ("-help", "this help");
  
  args.parse (argn, argv);
  
  std::vector<std::string> arguments = args.arguments ();

  if (arguments.size () < 1 || args.optionIsSet ("-help")) {
    printHelp (args);
    return -1;
  }
  if (arguments [0] == "dump") { // dump DB
    std::string what = arguments [1];
    std::string connect = args.getParameter ("-connect");
    std::string tag = args.getParameter ("-tag");
    std::string runStr = args.getParameter ("-run");
    std::string output = args.getParameter ("-output");
    unsigned run = runStr.empty () ? 1 : atoi (runStr.c_str());
    PoolData db (connect);
    if (what == "pedestals") {
      pool::Ref<HcalPedestals> ref = getDataFromDb<HcalPedestals> (db, tag, run);
      if (!ref.isNull ()) dumpData (ref, output);
    }
    else if (what == "gains") {
      pool::Ref<HcalGains> ref = getDataFromDb<HcalGains> (db, tag, run);
      if (!ref.isNull ()) dumpData (ref, output);
    }
    else {
      std::cerr << "ERROR object " << what << " is not supported" << std::endl;
    }
  }
  else if (arguments [0] == "fill") { // fill DB
    std::string what = arguments [1];
    std::string connect = args.getParameter ("-connect");
    std::string tag = args.getParameter ("-tag");
    std::string runStr = args.getParameter ("-run");
    std::string input = args.getParameter ("-input");
    unsigned run = runStr.empty () ? 0 : atoi (runStr.c_str());
    if (connect.empty ()) connect = "sqlite_file:hcalCalibrations_new.db";
    PoolData db (connect);
    if (what == "pedestals") {
      HcalPedestals* pedestals = new HcalPedestals ();
      readData (input, pedestals);
      db.StoreData (pedestals, run, tag, "HcalPedestals");
    }
    else if (what == "gains") {
      HcalGains* gains = new HcalGains ();
      readData (input, gains);
      db.StoreData (gains, run, tag, "HcalGains");
    }
  }
  else {
    std::cerr << "Unknown option. Try -help option for more details" << std::endl;
  }
  return 0;
}
