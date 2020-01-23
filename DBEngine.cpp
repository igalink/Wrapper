// DbEngine.cpp

#include "DBEngine.h"
#include <thrift/Thrift.h>
#include "Catalog/Catalog.h"
#include "QueryRunner/QueryRunner.h"
#include "Import/Importer.h"
#include "Shared/Logger.h"
#include "Shared/mapdpath.h"
#include <array>
#include <boost/filesystem.hpp>
#include <exception>
#include <iostream>
#include <memory>
#include <string>

#define CALCITEPORT 3279

bool g_enable_thrift_logs{ false };

namespace OmnisciDbEngine {
//////////////////////////////////////////////////////////////////////////// DBEngineImp
	class DBEngineImpl : public DBEngine {
	public:
		const std::string OMNISCI_DEFAULT_DB = "omnisci";
		const std::string OMNISCI_ROOT_USER = "admin";
		const std::string OMNISCI_DATA_PATH = "//mapd_data";
		//const int CALCITEPORT = 3279;

		//virtual ~DBEngineImpl() //override
		void Reset()
		{
			std::cout << "DESTRUCTOR DBEngineImpl" << std::endl;
			if (m_pQueryRunner)
				m_pQueryRunner->reset();
		}

		void Execute(const std::string& sQuery) //override
		{
			std::cout << "START EXECUTE : " << sQuery << std::endl;
			if (m_pQueryRunner != nullptr) {
				m_pQueryRunner->runDDLStatement(sQuery);
			}
			std::cout << "END EXECUTE" << std::endl;
		}

		DBEngineImpl(const std::string& sBasePath) 
		//: DBEngine()
		: m_sBasePath(sBasePath)
		, m_pQueryRunner(nullptr)
		{
			std::cout << "CONSTRUCTOR DBEngineImpl begin: " << sBasePath << std::endl;
			if (!g_enable_thrift_logs) {
			    apache::thrift::GlobalOutput.setOutputFunction([](const char* msg) {});
			}

			if (!boost::filesystem::exists(m_sBasePath)) {
			    std::cerr << "Catalog basepath " + m_sBasePath + " does not exist.\n";
			}
			else {
				MapDParameters mapdParms;
				std::string sDataPath = m_sBasePath + OMNISCI_DATA_PATH;
				std::cout << "CONSTRUCTOR DBEngineImpl data path = " << sDataPath << std::endl;
				m_DataMgr = std::make_shared<Data_Namespace::DataMgr>(sDataPath, mapdParms, false, 0);
				std::cout << "CONSTRUCTOR DBEngineImpl data manager created" << std::endl;
				auto calcite = std::make_shared<Calcite>(-1, CALCITEPORT, m_sBasePath, 1024);
				std::cout << "CONSTRUCTOR DBEngineImpl calcite created" << std::endl;
				auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
				sys_cat.init(m_sBasePath, m_DataMgr, {}, calcite, false, false, {});
				//if (!sys_cat) {
				//	std::cerr << "SysCatalog is null " << std::endl;
				//}
				//else 
					if (!sys_cat.getSqliteConnector()) {
					std::cerr << "SqliteConnector is null " << std::endl;
				}
				else {
					std::cout << "CONSTRUCTOR DBEngineImpl system catalog created" << std::endl;
					sys_cat.getMetadataForDB(OMNISCI_DEFAULT_DB, m_Database); //TODO: Check
					std::cout << "CONSTRUCTOR DBEngineImpl DB metadata created" << std::endl;
					auto catalog = Catalog_Namespace::Catalog::get(m_sBasePath, m_Database, m_DataMgr, std::vector<LeafHostInfo>(), calcite, false);
					std::cout << "CONSTRUCTOR DBEngineImpl catalog created" << std::endl;
					sys_cat.getMetadataForUser(OMNISCI_ROOT_USER, m_User);
					std::cout << "CONSTRUCTOR DBEngineImpl user metadata got" << std::endl;
					auto session = std::make_unique<Catalog_Namespace::SessionInfo>(catalog, m_User, ExecutorDeviceType::CPU, "");
					m_pQueryRunner = QueryRunner::QueryRunner::init(session);
					std::cout << "CONSTRUCTOR DBEngineImpl query runner created" << std::endl;
				}
			}
		} 

    private:
		std::string m_sBasePath;
		std::shared_ptr<Data_Namespace::DataMgr> m_DataMgr;
		Catalog_Namespace::DBMetadata m_Database;
		Catalog_Namespace::UserMetadata m_User;
		QueryRunner::QueryRunner* m_pQueryRunner;
    };

    DBEngine* DBEngine::Create(std::string sPath) {
	return new DBEngineImpl(sPath);
    }

    // downcasting methods
    inline DBEngineImpl * GetImpl(DBEngine* ptr) { return (DBEngineImpl *)ptr; }
    inline const DBEngineImpl * GetImpl(const DBEngine* ptr) { return (const DBEngineImpl *)ptr; }

    void DBEngine::Reset() {
	DBEngineImpl* pEngine = GetImpl(this);
	//pEngine->Reset();
    }

    void DBEngine::Execute(std::string sQuery) {
	DBEngineImpl* pEngine = GetImpl(this);
	pEngine->Execute(sQuery);
    }
}