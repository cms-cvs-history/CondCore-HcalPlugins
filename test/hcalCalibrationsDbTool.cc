#include <stdlib.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>

// other
#include "DataFormats/HcalDetId/interface/HcalDetId.h"
#include "DataFormats/HcalDetId/interface/HcalTrigTowerDetId.h"
#include "DataFormats/HcalDetId/interface/HcalElectronicsId.h"


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
#include "Collection/Collection.h"
#include "AttributeList/AttributeList.h"
#include "FileCatalog/FCSystemTools.h"
#include "FileCatalog/FCException.h"
#include "FileCatalog/IFCAction.h"

// conditions
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"

// Hcal calibrations
#include "CalibCalorimetry/HcalAlgos/interface/HcalDbServiceHardcode.h"
#include "CondFormats/HcalObjects/interface/HcalPedestals.h"
#include "CondFormats/HcalObjects/interface/HcalPedestalWidths.h"
#include "CondFormats/HcalObjects/interface/HcalGains.h"
#include "CondFormats/HcalObjects/interface/HcalGainWidths.h"
#include "CondFormats/HcalObjects/interface/HcalElectronicsMap.h"
#include "CondFormats/HcalObjects/interface/HcalQIEShape.h"
#include "CondFormats/HcalObjects/interface/HcalCannelQuality.h"
#include "CondFormats/HcalObjects/interface/HcalQIEData.h"

using namespace cms;

class Args {
 public:
  Args () {};
  ~Args () {};
  void defineOption (const std::string& fOption, const std::string& fComment = "");
  void defineParameter (const std::string& fParameter, const std::string& fComment = "");
  void parse (int nArgs, char* fArgs []);
  void printOptionsHelp () const;
  std::string command () const;
  std::vector<std::string> arguments () const;
  bool optionIsSet (const std::string& fOption) const;
  std::string getParameter (const std::string& fKey);
 private:
  std::string mProgramName;
  std::vector <std::string> mOptions;
  std::vector <std::string> mParameters;
  std::vector <std::string> mArgs;
  std::map <std::string, std::string> mParsed;
  std::map <std::string, std::string> mComments;
};

class PoolData {
 public:
  PoolData (const std::string& fConnect);
  ~PoolData ();

  template <class T>
  bool storeObject (T* fObject, const std::string& fContainer, pool::Ref<T>* fObject);

  template <class T>
  bool updateObject (T* fObject, pool::Ref<T>* fUpdate);

  template <class T>
  bool updateObject (pool::Ref<T>* fUpdate);

  template <class T>
  bool storeIOV (const pool::Ref<T>& fObject, unsigned fMaxRun, pool::Ref<cond::IOV>* fIov);

  template <class T>
  bool getObject (const pool::Ref<cond::IOV>& fIOV, unsigned fRun, pool::Ref<T>* fObject);

  template <class T> 
  bool getObject (const std::string& fToken, pool::Ref<T>* fObject);

  pool::IDataSvc* service () {return  mService;}
  std::string connect () {return mConnect;}
 private:
  std::string mConnect;
  pool::IFileCatalog mCatalog;
  pool::IDataSvc* mService;
  pool::Placement mPlacement;
};

  
template <class T> 
void dumpData (const T& fObject, const std::string& fOtput) {
  std::ofstream out (fOtput.c_str());
  char buffer [1024];
  sprintf (buffer, "# %4s %4s %4s %4s %8s %8s %8s %8s %10s\n", "eta", "phi", "dep", "det", "cap1", "cap2", "cap3", "cap4", "HcalDetId");
  out << buffer;
  std::vector<unsigned long> channels = fObject.getAllChannels ();
  for (std::vector<unsigned long>::iterator channel = channels.begin ();
       channel !=  channels.end ();
       channel++) {
    HcalDetId id ((uint32_t) *channel);
    const float* values = fObject.getValues (*channel);
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
  for (unsigned i = 0; i <= fLine.size (); i++) {
    if (fLine [i] == ' ' || i == fLine.size ()) {
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

    std::ifstream in (fInput.empty () ? "/dev/null" : fInput.c_str());
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

 void readData (const std::string& fInput, HcalElectronicsMap* fObject) {
    char buffer [1024];

    std::ifstream in (fInput.empty () ? "/dev/null" : fInput.c_str());
    while (in.getline(buffer, 1024)) {
      if (buffer [0] == '#') continue; //ignore comment
      std::vector <std::string> items = splitString (std::string (buffer));
      if (items.size () < 12) {
	if (items.size () > 0) {
	  std::cerr << "Bad line: " << buffer << "\n line must contain 12 items: i  cr sl tb dcc spigot fiber fiberchan subdet ieta iphi depth" << std::endl;
	}
	continue;
      }
      int crate = atoi (items [1].c_str());
      int slot = atoi (items [2].c_str());
      int top = 1;
      if (items [3] == "b") top = 0;
      int dcc = atoi (items [4].c_str());
      int spigot = atoi (items [5].c_str());
      int fiber = atoi (items [6].c_str());
      int fiberCh = atoi (items [7].c_str());
      HcalSubdetector subdet = HcalBarrel;
      if (items [8] == "HE") subdet = HcalEndcap;
      else if (items [8] == "HF") subdet = HcalForward;
      else if (items [8] == "HT") subdet = HcalTriggerTower;
      int eta = atoi (items [9].c_str());
      int phi = atoi (items [10].c_str());
      int depth = atoi (items [11].c_str());

      HcalElectronicsId elId (fiberCh, fiber, spigot, dcc);
      elId.setHTR (crate, slot, top);
      if (subdet == HcalTriggerTower) {
	HcalTrigTowerDetId trigId (eta, phi);
	fObject->mapEId2tId (elId (), trigId.rawId());
      }
      else {
	HcalDetId chId (subdet, eta, phi, depth);
	fObject->mapEId2chId (elId (), chId.rawId());
      }
    }
    fObject->sortByElectronicsId ();
  }

bool validHcalCell (const HcalDetId& fCell) {
  if (fCell.iphi () <=0)  return false;
  int absEta = abs (fCell.ieta ());
  int phi = fCell.iphi ();
  int depth = fCell.depth ();
  HcalSubdetector det = fCell.subdet ();
  // phi ranges
  if ((absEta >= 40 && phi > 18) ||
      (absEta >= 21 && phi > 36) ||
      phi > 72)   return false;
  if (absEta <= 0)       return false;
  else if (absEta <= 14) return (depth == 1 || depth == 4) && det == HcalBarrel; 
  else if (absEta == 15) return (depth == 1 || depth == 2 || depth == 4) && det == HcalBarrel; 
  else if (absEta == 16) return depth >= 1 && depth <= 2 && det == HcalBarrel || depth == 3 && det == HcalEndcap; 
  else if (absEta == 17) return depth == 1 && det == HcalEndcap; 
  else if (absEta <= 26) return depth >= 1 && depth <= 2 && det == HcalEndcap; 
  else if (absEta <= 28) return depth >= 1 && depth <= 3 && det == HcalEndcap; 
  else if (absEta == 29) return depth >= 1 && depth <= 2 && (det == HcalEndcap || det == HcalForward); 
  else if (absEta <= 41) return depth >= 1 && depth <= 2 && det == HcalForward;
  else return false;
}

template <class T>
std::vector<HcalDetId> undefinedCells (const T& fData) {
  static std::vector<HcalDetId> result;
  if (result.size () <= 0) {
    for (int eta = -50; eta < 50; eta++) {
      for (int phi = 0; phi < 100; phi++) {
	for (int depth = 1; depth < 5; depth++) {
	  for (int det = 1; det < 5; det++) {
	    HcalDetId cell ((HcalSubdetector) det, eta, phi, depth);
	    if ( validHcalCell(cell) && !fData.getValues (cell.rawId())) result.push_back (cell);
	  }
	}
      }
    }
  }
  return result;
}

void fillDefaults (HcalPedestals* fPedestals) {
  HcalDbServiceHardcode srv;
  std::vector<HcalDetId> cells = undefinedCells (*fPedestals);
  int i = cells.size ();
  while (--i >= 0) fPedestals->addValue (cells[i].rawId(), srv.pedestals (cells [i]));
  fPedestals->sort ();
}

void fillDefaults (HcalGains* fGains) {
  HcalDbServiceHardcode srv;
  std::vector<HcalDetId> cells = undefinedCells (*fGains);
  int i = cells.size ();
  while (--i >= 0) fGains->addValue (cells[i].rawId(), srv.gains (cells [i]));
  fGains->sort ();
}

void fillDefaults (HcalElectronicsMap* fMap) {
  std::cerr << "ERROR: fillDefaults (HcalElectronicsMap* fMap) is not implemented. Ignore." << std::endl;
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
  std::cout << "  where <what> is: \n    pedestals\n    gains\n    emap\n" << std::endl;
  args.printOptionsHelp ();
}



int main (int argn, char* argv []) {

  Args args;
  args.defineParameter ("-connect", "DB connection string, POOL format");
  args.defineParameter ("-input", "input file where to get constants from");
  args.defineParameter ("-output", "output file where to dump constants to");
  args.defineParameter ("-run", "run # for which constands should be dumped");
  args.defineParameter ("-tag", "tag for the constants set");
  args.defineOption ("-help", "this help");
  args.defineOption ("-defaults", "fill default values for not specifyed cells");
  
  args.parse (argn, argv);
  
  std::vector<std::string> arguments = args.arguments ();

  if (arguments.size () < 2 || args.optionIsSet ("-help")) {
    printHelp (args);
    return -1;
  }

  bool dump = arguments [0] == "dump";
  bool fill = arguments [0] == "fill";
  bool add = arguments [0] == "add";
  bool getPedestals = arguments [1] == "pedestals";
  bool getGains = arguments [1] == "gains";
  bool emap = arguments [1] == "emap";
  bool defaults = args.optionIsSet ("-defaults");

  std::string what = arguments [1];

  std::string connect = args.getParameter ("-connect");
  if (connect.empty ()) {
    std::cerr << "ERROR: -connect is mandatory parameter" << std::endl;
    return -1;
  }
  std::string tag = args.getParameter ("-tag");
  if (tag.empty ()) {
    std::cerr << "ERROR: -tag is mandatory parameter" << std::endl;
    return -1;
  }
  std::string runStr = args.getParameter ("-run");
  if (runStr.empty ()) runStr = "0";
  unsigned run = atoi (runStr.c_str());
  
  std::string metadataToken; // need to separate RAL and ORA operations

  if (dump || add) { // get metadata
    cond::MetaData md (connect);
    metadataToken = md.getToken (tag);
    if (metadataToken.empty ()) {
      std::cerr << "ERROR: can not find metadata for tag " << tag << std::cerr;
      return 2;
    }
  }
  {
    PoolData db (connect);
    
    // get IOV object
    pool::Ref<cond::IOV> iov;
    if (!metadataToken.empty ()) {
      db.getObject (metadataToken, &iov);
      if (iov.isNull ()) {
	std::cerr << "ERROR: can not find IOV for token " << metadataToken << std::endl;;
	return 2;
      }
    }
    
    if (dump) { // dump DB
      std::string output = args.getParameter ("-output");
      if (getPedestals) {
	pool::Ref<HcalPedestals> ref;
	if (db.getObject (iov, run, &ref)) dumpData (*ref, output);
      }
      else if (getGains) {
	pool::Ref<HcalGains> ref;
	if (db.getObject (iov, run, &ref)) dumpData (*ref, output);
      }
      else {
	std::cerr << "ERROR object " << what << " is not supported" << std::endl;
      }
    }
    else if (fill || add) { // fill DB
      std::string input = args.getParameter ("-input");
      
      if (getPedestals) {
	HcalPedestals* pedestals = new HcalPedestals ();
	readData (input, pedestals);
	if (defaults) fillDefaults (pedestals); 
	pool::Ref<HcalPedestals> ref;
	if (!db.storeObject (pedestals, "HcalPedestals", &ref) ||
	    !db.storeIOV (ref, run, &iov)) {
	  std::cerr << "ERROR: failed to store object or its IOV" << std::endl;
	  return 1;
	}
      }
      else if (getGains) {
	HcalGains* gains = new HcalGains ();
	readData (input, gains);
	pool::Ref<HcalGains> ref;
	if (defaults) fillDefaults (gains); 
	if (!db.storeObject (gains, "HcalGains", &ref) ||
	    !db.storeIOV (ref, run, &iov)) {
	  std::cerr << "ERROR: failed to store object or its IOV" << std::endl;
	  return 1;
	}
      }
      else if (emap) {
	HcalElectronicsMap* map = new HcalElectronicsMap ();
	readData (input, map);
	pool::Ref<HcalElectronicsMap> ref;
	if (defaults) fillDefaults (map); 
	if (!db.storeObject (map, "HcalElectronicsMap", &ref) ||
	    !db.storeIOV (ref, run, &iov)) {
	  std::cerr << "ERROR: failed to store object or its IOV" << std::endl;
	  return 1;
	}
      }
      metadataToken = iov.toString ();
    }
    else {
      std::cerr << "Unknown option. Try -help option for more details" << std::endl;
    }
  }
  
  if (fill) { // update metadata
    cond::MetaData md (connect);
    md.addMapping (tag, metadataToken);
  }
  return 0;
}



//==================== Args ===== BEGIN ==============================
void Args::defineOption (const std::string& fOption, const std::string& fComment) {
  mOptions.push_back (fOption);
  mComments [fOption] = fComment;
}

void Args::defineParameter (const std::string& fParameter, const std::string& fComment) {
  mParameters.push_back (fParameter);
  mComments [fParameter] = fComment;
}

void Args::parse (int nArgs, char* fArgs []) {
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

void Args::printOptionsHelp () const {
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

std::string Args::command () const {
  int ipos = mProgramName.rfind ('/');
  return std::string (mProgramName, ipos+1);
}

std::vector<std::string> Args::arguments () const {return mArgs;}

bool Args::optionIsSet (const std::string& fOption) const {
  return mParsed.find (fOption) != mParsed.end ();
}

std::string Args::getParameter (const std::string& fKey) {
  if (optionIsSet (fKey)) return mParsed [fKey];
  return "";
}
//==================== Args ===== END ==============================

//==================== PoolData ===== BEGIN ==============================
PoolData::PoolData (const std::string& fConnect) 
  : mConnect (fConnect)
{
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

PoolData::~PoolData () {
  mService->session().disconnectAll();
  mCatalog.commit ();
  mCatalog.disconnect ();
  delete mService;
}

template <class T>
bool PoolData::storeObject (T* fObject, const std::string& fContainer, pool::Ref<T>* fRef) {
  if (!fRef->isNull ()) {
    std::cerr << "PoolData::storeObject-> Ref is not empty. Ignore." << std::endl;
    return false;
  }
  try {
    mService->transaction().start(pool::ITransaction::UPDATE);
    *fRef = pool::Ref <T> (mService, fObject);
    mPlacement.setContainerName (fContainer);
    fRef->markWrite (mPlacement);
    mService->transaction().commit();
  }
  catch (...) {
    std::cerr << "PoolData::storeObject  error "  << std::endl; 
    return false;
  }
  return true;
} 

template <class T>
bool PoolData::updateObject (T* fObject, pool::Ref<T>* fUpdate) {
  try {
    mService->transaction().start(pool::ITransaction::UPDATE);
    if (fObject) *(fUpdate->ptr ()) = *fObject; // update object
    fUpdate->markUpdate();
    mService->transaction().commit();
  }
  catch (...) {
    std::cerr << "PoolData::updateObject  error "  << std::endl;
    return false;
  }
  return true;
}

template <class T>
bool PoolData::updateObject (pool::Ref<T>* fUpdate) {
  return updateObject ((T*)0, fUpdate);
}

template <class T>
bool PoolData::storeIOV (const pool::Ref<T>& fObject, unsigned fMaxRun, pool::Ref<cond::IOV>* fIov) {
  unsigned maxRun = fMaxRun == 0 ? 0xffffffff : fMaxRun;
  if (fIov->isNull ()) {
    cond::IOV* newIov = new cond::IOV ();
    newIov->iov.insert (std::make_pair (maxRun, fObject.toString ()));
    return storeObject (newIov, "IOV", fIov);
  }
  else {
    (*fIov)->iov.insert (std::make_pair (maxRun, fObject.toString ()));
    return updateObject (fIov);
  }
}

template <class T>
bool PoolData::getObject (const pool::Ref<cond::IOV>& fIOV, unsigned fRun, pool::Ref<T>* fObject) {
  if (!fIOV.isNull ()) {
    // scan IOV, search for valid data
    for (std::map<unsigned long,std::string>::iterator iovi = fIOV->iov.begin (); iovi != fIOV->iov.end (); iovi++) {
      if (fRun <= iovi->first) {
	std::string token = iovi->second;
	return getObject (token, fObject);
      }
    }
    std::cerr << "PoolData::getObject-> no object for run " << fRun << " is found" << std::endl;
  }
  else {
    std::cerr << "PoolData::getObject-> IOV reference is not set" << std::endl;
  }
  return false;
}

template <class T> 
bool PoolData::getObject (const std::string& fToken, pool::Ref<T>* fObject) {
  service ()->transaction().start(pool::ITransaction::READ);
  try {
    *fObject = pool::Ref <T> (service (), fToken);
    mService->transaction().commit();
  }
  catch( const pool::RelationalTableNotFound& e ){
    std::cerr << "PoolData::getObject-> pool::RelationalTableNotFound Exception" << std::endl;
  }
  catch (const seal::Exception& e) {
    std::cerr<<"PoolData::getObject-> seal exception: " << e.what() << std::endl;
  }
  catch(...){
    std::cerr << "PoolData::getObject-> Funny error" << std::endl;
  }
  return !(fObject->isNull ());
}

//============== PoolData ===== END =====================
