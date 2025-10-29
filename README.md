#Example 

#\_cfg.login - email 

#\_cfg.token - token key 

#\_cfg.robot_id - robot id 

#\_cfg.portofolio_name - not used, does not affect 

#\_cfg.fkviking_role - user role 

#=\_cfg.fkviking_sec_type - default sec type, 4 = MOEX_FOND

#cfg = NewCfg()

positions, money = getViking(cfg)

def getViking(\_cfg)  
  import asyncio  
  *cfg.bp = FkVikingPy(login=*cfg.login, token=\_cfg.token, r_id=\_cfg.robot_id, p_id=\_cfg.portofolio_name, role=\_cfg.fkviking_role, sec_type=\_cfg.fkviking_sec_type)  
  [asyncio.run](http://asyncio.run)(\_[cfg.bp.run](http://cfg.bp.run)())  
  return *cfg.bp.positions,* [cfg.bp.money](http://cfg.bp.money)
